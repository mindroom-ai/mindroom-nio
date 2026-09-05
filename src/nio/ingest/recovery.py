"""Bounded Classic recovery phases replayed through ordinary journal frames."""

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import replace
from typing import Any
from urllib.parse import quote

from ._json import canonical_json, load_json
from .membership import _latest_own_membership, _membership_observation
from .model import TransportKind
from .ports import (
    MAX_CANONICAL_STAGED_RESPONSE_BODY_BYTES,
    NetworkRequest,
    NetworkResult,
)
from .source import (
    ClassicCursor,
    RoomSection,
    SyncFrame,
    _classic_cursor_from_json,
    canonical_classic_cursor,
    require_json_events,
    require_json_object,
    validate_network_result_identity,
)

_MAX_PAGES = 1000
_PAGE_SIZE = 100
_MAX_PAGE_BYTES = 2 * 1024 * 1024


def bounded_classic_request(
    request: NetworkRequest, limit: int
) -> tuple[NetworkRequest, int]:
    if request.transport is not TransportKind.CLASSIC:
        return request, limit
    value = require_json_object(
        load_json(dict(request.query)["filter"].encode(), "sync filter"), "sync filter"
    )
    room = require_json_object(value.setdefault("room", {}), "room filter")
    timeline = require_json_object(room.setdefault("timeline", {}), "timeline filter")
    requested = timeline.get("limit", limit)
    if type(requested) is not int or requested < 0:
        raise ValueError("timeline limit must be a nonnegative integer")
    effective = min(requested, limit)
    timeline["limit"] = effective
    query = tuple(
        (key, canonical_json(value).decode() if key == "filter" else item)
        for key, item in request.query
    )
    return replace(request, query=query), effective


def validate_classic_recovery_request(request: NetworkRequest) -> str:
    value = load_json(dict(request.query)["filter"].encode(), "sync filter")
    timeline_filter = dict(value.get("room", {}).get("timeline", {}))
    timeline_filter.pop("limit", None)
    if set(timeline_filter) - {"lazy_load_members", "include_redundant_members"}:
        raise ValueError("filtered timelines cannot prove recovery state ordering")
    return canonical_json(timeline_filter).decode()


def recovery_progress(cursor_json: bytes) -> dict[str, Any] | None:
    cursor = _classic_cursor_from_json(cursor_json)
    if cursor.recovery_json is None:
        return None
    value = require_json_object(
        load_json(cursor.recovery_json, "recovery progress"), "recovery progress"
    )
    if set(value) != {
        "source_sha256",
        "rooms",
        "phase",
        "room_index",
        "page_count",
        "token",
        "seen_tokens",
        "previous_event_ids",
    }:
        raise ValueError("recovery progress fields are invalid")
    digest, rooms = value["source_sha256"], value["rooms"]
    if (
        type(digest) is not str
        or len(digest) != 64
        or any(char not in "0123456789abcdef" for char in digest)
    ):
        raise ValueError("recovery source digest is invalid")
    if (
        type(rooms) is not list
        or not rooms
        or any(type(room) is not str or not room for room in rooms)
        or len(set(rooms)) != len(rooms)
    ):
        raise ValueError("recovery rooms are invalid")
    phase, index, count = value["phase"], value["room_index"], value["page_count"]
    if type(phase) is not str or phase not in ("prologue", "page", "tail"):
        raise ValueError("recovery phase is invalid")
    if (
        type(index) is not int
        or not 0 <= index <= len(rooms)
        or (index == len(rooms)) != (phase == "tail")
    ):
        raise ValueError("recovery room position is invalid")
    if type(count) is not int or not 0 <= count <= _MAX_PAGES:
        raise ValueError("recovery page count is invalid")
    token, seen, previous = (
        value["token"],
        value["seen_tokens"],
        value["previous_event_ids"],
    )
    if type(token) is not str or not token:
        raise ValueError("recovery pagination token is invalid")
    for items, maximum in ((seen, _MAX_PAGES + 1), (previous, _PAGE_SIZE)):
        if (
            type(items) is not list
            or len(items) > maximum
            or any(type(item) is not str or not item for item in items)
            or len(set(items)) != len(items)
        ):
            raise ValueError("recovery pagination history is invalid")
    if (
        not seen
        or seen[0] != cursor.next_batch
        or seen[-1] != token
        or len(seen) > count + 1
    ):
        raise ValueError("recovery pagination history does not match position")
    if phase == "prologue" and (
        index or count or previous or seen != [cursor.next_batch]
    ):
        raise ValueError("recovery prologue position is invalid")
    return value


def _cursor(frame: SyncFrame, progress: dict[str, Any]) -> bytes:
    return canonical_classic_cursor(
        ClassicCursor(
            _classic_cursor_from_json(frame.request_cursor_json).next_batch,
            canonical_json(progress),
        )
    )


def start_classic_recovery(frame: SyncFrame, rooms: tuple[str, ...]) -> bytes:
    if frame.origin.transport is not TransportKind.CLASSIC:
        raise ValueError("history capture requires Classic sync")
    cursor = _classic_cursor_from_json(frame.request_cursor_json)
    if not cursor.next_batch or cursor.recovery_json is not None or not rooms:
        raise ValueError("history capture requires an idle incremental source")
    progress = {
        "source_sha256": frame.source_sha256.hex(),
        "rooms": list(rooms),
        "phase": "prologue",
        "room_index": 0,
        "page_count": 0,
        "token": cursor.next_batch,
        "seen_tokens": [cursor.next_batch],
        "previous_event_ids": [],
    }
    result = _cursor(frame, progress)
    _progress_frame(replace(frame, request_cursor_json=result))
    return result


def _progress_frame(frame: SyncFrame) -> dict[str, Any]:
    progress = recovery_progress(frame.request_cursor_json)
    if progress is None or progress["source_sha256"] != frame.source_sha256.hex():
        raise ValueError("history recovery source does not match continuation")
    segments = {segment.room_id: segment for segment in frame.room_segments}
    for room_id in progress["rooms"]:
        segment = segments.get(room_id)
        if (
            segment is None
            or segment.initial
            or not segment.timeline_limited
            or segment.section not in (RoomSection.JOIN, RoomSection.LEAVE)
            or not segment.timeline_prev_batch
        ):
            raise ValueError("history recovery room does not match source interval")
    return progress


def _page(
    value: Any, start: str, seen: set[str]
) -> tuple[list[dict[str, Any]], str | None]:
    value = require_json_object(value, "history page")
    if value.get("start") != start:
        raise ValueError("history page start does not match its request")
    events = require_json_events(value.get("chunk"), "history chunk")
    if len(events) > _PAGE_SIZE:
        raise ValueError("history page exceeds event bound")
    end = value.get("end")
    if end is not None and (type(end) is not str or not end or end in seen):
        raise ValueError("history pagination did not advance")
    for event in events:
        if type(event.get("event_id")) is not str or not event["event_id"]:
            raise ValueError("history event has no stable identity")
    return events, end


async def capture_classic_recovery(
    request: NetworkRequest,
    frame: SyncFrame,
    response_body: bytes,
    rooms: tuple[str, ...],
    fetch: Callable[[NetworkRequest], Awaitable[NetworkResult]],
) -> bytes | None:
    if frame.origin.transport is not TransportKind.CLASSIC:
        return None
    progress = recovery_progress(request.request_cursor_json)
    if progress is None:
        return None
    if request.request_cursor_json != frame.request_cursor_json:
        raise ValueError("history recovery request cursor does not match frame")
    progress = _progress_frame(frame)
    if tuple(progress["rooms"]) != rooms:
        raise ValueError("history recovery rooms do not match continuation")
    if progress["phase"] != "page":
        return None
    if progress["page_count"] >= _MAX_PAGES:
        raise ValueError("history recovery page limit exceeded")
    room_id = rooms[progress["room_index"]]
    segment = next(
        segment for segment in frame.room_segments if segment.room_id == room_id
    )
    token, target = progress["token"], segment.timeline_prev_batch
    assert target is not None
    if token == target:
        return canonical_json({"start": token, "chunk": []})
    page_filter = validate_classic_recovery_request(request)
    limit = _PAGE_SIZE
    while True:
        page_request = replace(
            request,
            path=f"/_matrix/client/v3/rooms/{quote(room_id, safe='')}/messages",
            query=(
                ("from", token),
                ("to", target),
                ("dir", "f"),
                ("limit", str(limit)),
                ("filter", page_filter),
            ),
        )
        for attempt in range(3):
            result = await fetch(page_request)
            validate_network_result_identity(page_request, result)
            if result.status_code == 200:
                break
            retryable = (
                result.failure is not None
                or result.status_code in (408, 429)
                or result.status_code is not None
                and result.status_code >= 500
            )
            if not retryable or attempt == 2:
                raise ValueError("history recovery request failed")
            await asyncio.sleep(
                min(result.retry_after_ms / 1000, 5)
                if result.retry_after_ms is not None
                else 0.1 * 2**attempt
            )
        evidence = None
        if len(result.body) <= _MAX_PAGE_BYTES:
            value = require_json_object(
                load_json(result.body, "history page"), "history page"
            )
            events = require_json_events(value.get("chunk"), "history chunk")
            if len(events) <= _PAGE_SIZE:
                evidence = canonical_json(value)
        if (
            evidence is not None
            and len(evidence) <= _MAX_PAGE_BYTES
            and len(response_body) + len(evidence)
            <= MAX_CANONICAL_STAGED_RESPONSE_BODY_BYTES
        ):
            _page(value, token, set(progress["seen_tokens"]))
            return evidence
        if limit == 1:
            raise ValueError("history recovery page exceeds byte or event limit")
        limit = max(1, limit // 2)


def apply_classic_recovery(
    frame: SyncFrame, evidence_json: bytes | None, own_user_id: str
) -> SyncFrame:
    if frame.origin.transport is not TransportKind.CLASSIC:
        if evidence_json is not None:
            raise ValueError("history capture requires Classic sync")
        return frame
    progress = recovery_progress(frame.request_cursor_json)
    if progress is None:
        if evidence_json is not None:
            raise ValueError("history capture has no continuation")
        return frame
    progress = _progress_frame(frame)
    phase = progress["phase"]
    if phase != "page" and evidence_json is not None:
        raise ValueError("history evidence requires a page phase")
    if phase == "prologue":
        progress["phase"] = "page"
        return replace(
            frame,
            candidate_cursor_json=_cursor(frame, progress),
            room_segments=(),
            ephemeral_json=(),
            global_account_data_json=(),
        )
    frame = replace(
        frame,
        to_device_json=(),
        device_list_delta_json=b'{"changed":[],"left":[]}',
        one_time_key_counts_json=b"{}",
        unused_fallback_key_types_json=b"[]",
    )
    if phase == "tail":
        return replace(
            frame,
            room_segments=tuple(
                (
                    replace(segment, timeline_limited=False)
                    if segment.room_id in progress["rooms"]
                    else segment
                )
                for segment in frame.room_segments
            ),
        )
    if evidence_json is None or len(evidence_json) > _MAX_PAGE_BYTES:
        raise ValueError("history page evidence is missing or oversized")
    if progress["page_count"] >= _MAX_PAGES:
        raise ValueError("history recovery page limit exceeded")
    room_id = progress["rooms"][progress["room_index"]]
    segment = next(
        segment for segment in frame.room_segments if segment.room_id == room_id
    )
    events, end = _page(
        load_json(evidence_json, "history page"),
        progress["token"],
        set(progress["seen_tokens"]),
    )
    seen_ids = {
        load_json(payload, "timeline event").get("event_id")
        for payload in segment.timeline_json
    }
    seen_ids.update(progress["previous_event_ids"])
    recovered = []
    for event in events:
        if event.get("room_id", room_id) != room_id:
            raise ValueError("history event belongs to another room")
        if event["event_id"] not in seen_ids:
            recovered.append(event)
            seen_ids.add(event["event_id"])
    observation = _membership_observation(
        None,
        _latest_own_membership([], recovered, len(recovered), own_user_id),
        is_initial=False,
        is_expanded_timeline=False,
    )
    page_segment = replace(
        segment,
        section=RoomSection.UNCHANGED,
        state_json=(),
        timeline_json=tuple(canonical_json(event) for event in recovered),
        room_account_data_json=(),
        timeline_limited=False,
        timeline_prev_batch=None,
        live_event_count=0,
        recovered_event_count=len(recovered),
        membership_observation=replace(observation, is_live=False),
    )
    progress["page_count"] += 1
    if end is None or end == segment.timeline_prev_batch:
        progress["room_index"] += 1
        progress["phase"] = (
            "tail" if progress["room_index"] == len(progress["rooms"]) else "page"
        )
        progress["token"] = _classic_cursor_from_json(
            frame.request_cursor_json
        ).next_batch
        progress["seen_tokens"] = [progress["token"]]
        progress["previous_event_ids"] = []
    else:
        progress["token"] = end
        progress["seen_tokens"].append(end)
        if events:
            progress["previous_event_ids"] = list(
                dict.fromkeys(event["event_id"] for event in events)
            )
    return replace(
        frame,
        candidate_cursor_json=_cursor(frame, progress),
        room_segments=(page_segment,),
        ephemeral_json=(),
        global_account_data_json=(),
    )
