"""Direct tests for sliding membership and recovery-window decisions."""

import pytest

from nio import (
    RoomMemberEvent,
    RoomMessageText,
    SlidingSyncResponse,
    SlidingSyncRoom,
    SlidingSyncStateStub,
)
from nio.client.sliding_membership import (
    plan_sliding_prev_batches,
    sliding_membership_continues,
    sliding_membership_proof,
    sliding_recovery_cursor,
    sliding_room_is_invite,
    sliding_unrecoverable_discontinuity_room_ids,
)
from nio.sliding_sync_tokens import SlidingWindowToken

ROOM_A = "!a:example.org"
ROOM_B = "!b:example.org"
OWN_ID = "@own:example.org"


def window_token(
    token: str, membership_event_id: str | None = "$membership"
) -> SlidingWindowToken:
    return SlidingWindowToken(token, membership_event_id)


def own_member_event(
    event_id: str,
    membership: str = "join",
    *,
    prev_membership: str | None = None,
    replaces_state: str | None = None,
) -> RoomMemberEvent:
    """Our own membership event, optionally linked to the one it replaces."""
    unsigned: dict = {}
    if prev_membership is not None:
        unsigned["prev_content"] = {"membership": prev_membership}
    if replaces_state is not None:
        unsigned["replaces_state"] = replaces_state
    source = {
        "content": {"membership": membership},
        "event_id": event_id,
        "sender": OWN_ID,
        "state_key": OWN_ID,
        "origin_server_ts": 0,
        "room_id": ROOM_A,
        "type": "m.room.member",
    }
    if unsigned:
        source["unsigned"] = unsigned
    event = RoomMemberEvent.from_dict(source)
    assert isinstance(event, RoomMemberEvent)
    return event


def text_event(event_id: str) -> RoomMessageText:
    event = RoomMessageText.from_dict(
        {
            "content": {"body": event_id, "msgtype": "m.text"},
            "event_id": event_id,
            "sender": "@sender:example.org",
            "origin_server_ts": 0,
            "room_id": ROOM_A,
            "type": "m.room.message",
        }
    )
    assert isinstance(event, RoomMessageText)
    return event


def proof_room(
    *,
    required_state: list | None = None,
    timeline: list | None = None,
    initial: bool = False,
    limited: bool = False,
    num_live: int | None = None,
    prev_batch: str | None = None,
) -> SlidingSyncRoom:
    return SlidingSyncRoom(
        membership="join",
        initial=initial,
        limited=limited,
        required_state=list(required_state or []),
        timeline=list(timeline or []),
        num_live=num_live,
        prev_batch=prev_batch,
    )


@pytest.mark.parametrize(
    ("held", "room", "expected"),
    [
        pytest.param(
            None,
            proof_room(required_state=[own_member_event("$leave", "leave")]),
            None,
            id="explicit_membership_loss_fails_closed",
        ),
        pytest.param(
            None,
            proof_room(required_state=[own_member_event("$m1")]),
            "$m1",
            id="no_held_baseline_accepts_current_membership",
        ),
        pytest.param(
            "$m1",
            proof_room(required_state=[own_member_event("$m1")]),
            "$m1",
            id="exact_match_with_held_baseline",
        ),
        pytest.param(
            "$old",
            proof_room(timeline=[own_member_event("$new")]),
            "$new",
            id="live_own_join_supersedes_mismatched_baseline",
        ),
        pytest.param(
            "$m1",
            proof_room(
                required_state=[
                    own_member_event(
                        "$m2", prev_membership="join", replaces_state="$m1"
                    )
                ]
            ),
            "$m2",
            id="linked_join_to_join_rotation",
        ),
        pytest.param(
            "$m1",
            proof_room(
                required_state=[
                    own_member_event(
                        "$m2", prev_membership="join", replaces_state="$other"
                    )
                ]
            ),
            None,
            id="rotation_replacing_a_different_event_fails_closed",
        ),
        pytest.param(
            "$m1",
            proof_room(
                required_state=[
                    own_member_event(
                        "$m2", prev_membership="invite", replaces_state="$m1"
                    )
                ]
            ),
            None,
            id="rotation_from_non_join_fails_closed",
        ),
        pytest.param(
            "$m1",
            proof_room(required_state=[own_member_event("$m2")]),
            None,
            id="required_state_join_alone_does_not_date_a_rejoin",
        ),
        pytest.param(
            "$m1",
            proof_room(required_state=[SlidingSyncStateStub("m.room.member", OWN_ID)]),
            None,
            id="unparsed_membership_stub_fails_closed",
        ),
        pytest.param(
            "$m1",
            proof_room(),
            "$m1",
            id="no_membership_delta_keeps_held_baseline",
        ),
        pytest.param(
            "$m1",
            proof_room(initial=True),
            None,
            id="initial_window_without_membership_proof_fails_closed",
        ),
        pytest.param(
            None,
            proof_room(),
            None,
            id="no_membership_delta_and_no_held_baseline",
        ),
        pytest.param(
            "$m1",
            proof_room(
                required_state=[own_member_event("$m2")],
                timeline=[own_member_event("$historical"), text_event("$t")],
                initial=True,
                num_live=1,
            ),
            None,
            id="historical_join_outside_num_live_is_not_a_live_join",
        ),
    ],
)
def test_membership_proof(held, room, expected) -> None:
    tokens = {ROOM_A: window_token("w1", held)} if held is not None else {}

    assert (
        sliding_membership_proof(
            ROOM_A,
            room,
            user_id=OWN_ID,
            window_tokens=tokens,
        )
        == expected
    )


def test_live_own_join_proves_membership_but_breaks_continuity() -> None:
    tokens = {ROOM_A: window_token("w1", "$old")}
    room = proof_room(timeline=[own_member_event("$new")])

    assert (
        sliding_membership_proof(
            ROOM_A,
            room,
            user_id=OWN_ID,
            window_tokens=tokens,
        )
        == "$new"
    )
    assert not sliding_membership_continues(
        ROOM_A,
        room,
        user_id=OWN_ID,
        window_tokens=tokens,
    )


def test_unchanged_membership_continues() -> None:
    tokens = {ROOM_A: window_token("w1", "$m1")}
    room = proof_room(required_state=[own_member_event("$m1")])

    assert sliding_membership_continues(
        ROOM_A,
        room,
        user_id=OWN_ID,
        window_tokens=tokens,
    )


def test_stripped_state_without_membership_field_is_an_invite() -> None:
    room = SlidingSyncRoom(
        stripped_state=[SlidingSyncStateStub("m.room.member", OWN_ID)]
    )

    assert sliding_room_is_invite(room)


def test_recovery_cursor_uses_held_baseline_for_continuing_membership() -> None:
    room = proof_room(
        required_state=[own_member_event("$m1")],
        limited=True,
        prev_batch="w2",
    )

    assert (
        sliding_recovery_cursor(
            ROOM_A,
            room,
            user_id=OWN_ID,
            window_tokens={ROOM_A: window_token("w1", "$m1")},
        )
        == "w1"
    )


def test_unrecoverable_discontinuities_exclude_non_current_rooms() -> None:
    response = SlidingSyncResponse(
        "p1",
        rooms={ROOM_A: proof_room(limited=True)},
    )
    tokens = {ROOM_A: window_token("w1", "$m1")}

    assert (
        sliding_unrecoverable_discontinuity_room_ids(
            response,
            user_id=OWN_ID,
            window_tokens=tokens,
            known_room_ids=(),
            current_room_ids=frozenset(),
        )
        == frozenset()
    )
    assert sliding_unrecoverable_discontinuity_room_ids(
        response,
        user_id=OWN_ID,
        window_tokens=tokens,
        known_room_ids=(),
        current_room_ids=frozenset({ROOM_A}),
    ) == frozenset({ROOM_A})


def test_prev_batch_plan_excludes_non_current_rooms() -> None:
    response = SlidingSyncResponse(
        "p1",
        rooms={
            ROOM_A: proof_room(
                required_state=[own_member_event("$a")],
                prev_batch="a2",
            ),
            ROOM_B: proof_room(
                required_state=[own_member_event("$b")],
                prev_batch="b2",
            ),
        },
    )
    tokens = {
        ROOM_A: window_token("a1", "$a"),
        ROOM_B: window_token("b1", "$b"),
    }

    recorded, forgotten = plan_sliding_prev_batches(
        response,
        user_id=OWN_ID,
        window_tokens=tokens,
        current_room_ids=frozenset({ROOM_B}),
    )

    assert recorded == {ROOM_B: window_token("b2", "$b")}
    assert forgotten == []


def test_prev_batch_plan_forgets_old_baseline_before_recording_live_rejoin() -> None:
    response = SlidingSyncResponse(
        "p1",
        rooms={
            ROOM_A: proof_room(
                timeline=[own_member_event("$new")],
                prev_batch="w2",
            )
        },
    )

    recorded, forgotten = plan_sliding_prev_batches(
        response,
        user_id=OWN_ID,
        window_tokens={ROOM_A: window_token("w1", "$old")},
        current_room_ids=frozenset({ROOM_A}),
    )

    assert recorded == {ROOM_A: window_token("w2", "$new")}
    assert forgotten == [ROOM_A]
