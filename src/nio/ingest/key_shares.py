"""Retain interactive approvals and their handoff to ordinary preparation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from ..event_builders import ToDeviceMessage
from ..events import RoomKeyRequest, ToDeviceEvent
from ..exceptions import LocalProtocolError
from ._json import load_json
from .errors import JournalCapacityError, JournalIntegrityError
from .source import canonical_json

if TYPE_CHECKING:
    from ..client.async_client import AsyncClient
    from ..store._sync_journal import SqliteIngestionJournal
    from .state import OwnerView


@dataclass(frozen=True)
class _KeyShare:
    request_json: bytes
    state: str = "pending"
    message_json: bytes | None = None

    def request(self) -> RoomKeyRequest:
        event = ToDeviceEvent.parse_event(load_json(self.request_json, "key request"))
        if type(event) is not RoomKeyRequest:
            raise JournalIntegrityError("retained key-share request is invalid")
        return event

    def message(self) -> ToDeviceMessage:
        assert self.message_json is not None
        value = load_json(self.message_json, "key-share message")
        return ToDeviceMessage(**value)


class _OwnedKeyShares:
    def __init__(self, client: AsyncClient, journal: SqliteIngestionJournal):
        self.client = client
        self.journal = journal

    def _load(self, owner: OwnerView) -> dict[str, _KeyShare]:
        entries: dict[str, _KeyShare] = {}
        count, size, largest = self.journal._execute(
            "SELECT COUNT(*), COALESCE(SUM(LENGTH(payload)), 0), "
            "COALESCE(MAX(LENGTH(payload)), 0) FROM NioIngestKeyShare"
        ).fetchone()
        if count > 20_000 or size > 64 * 1024 * 1024 or largest > 1024 * 1024:
            raise JournalIntegrityError("retained key shares exceed capacity")
        if not count:
            return entries
        rows = self.journal._execute("SELECT * FROM NioIngestKeyShare").fetchall()
        for row in rows:
            if (
                row["account_id"] != owner.account_id
                or type(row["updated_revision"]) is not int
                or not 1 <= row["updated_revision"] <= owner.revision
            ):
                raise JournalIntegrityError(
                    "retained key-share owner or revision is invalid"
                )
            try:
                plain = self.journal._payload(
                    owner,
                    "NioIngestKeyShare",
                    row["payload"],
                    row["payload_sha256"],
                    header=canonical_json([row["request_id"], row["updated_revision"]]),
                )
                value = load_json(plain, "retained key share")
                if type(value) is not dict or set(value) != {
                    "request",
                    "state",
                    "message",
                }:
                    raise ValueError("invalid key-share fields")
                entry = _KeyShare(
                    canonical_json(value["request"]),
                    value["state"],
                    (
                        None
                        if value["message"] is None
                        else canonical_json(value["message"])
                    ),
                )
                self._validate(entry, owner, row["request_id"])
                entries[row["request_id"]] = entry
            except (TypeError, ValueError, KeyError) as error:
                raise JournalIntegrityError("retained key share is invalid") from error
        return entries

    @staticmethod
    def _validate(entry: _KeyShare, owner: OwnerView, request_id: str) -> None:
        request = entry.request()
        if (
            request.request_id != request_id
            or request.sender != owner.account_id
            or request.algorithm != "m.megolm.v1.aes-sha2"
        ):
            raise ValueError("retained key-share identity is invalid")
        if entry.state not in {"pending", "waiting", "claiming", "message"} or (
            entry.state == "message"
        ) != (entry.message_json is not None):
            raise ValueError("retained key-share state is invalid")
        if entry.message_json is not None:
            value = load_json(entry.message_json, "key-share message")
            if type(value) is not dict or set(value) != {
                "type",
                "recipient",
                "recipient_device",
                "content",
            }:
                raise ValueError("retained key-share message is invalid")
            message = entry.message()
            if (
                (message.type, message.recipient, message.recipient_device)
                != ("m.room.encrypted", request.sender, request.requesting_device_id)
                or type(message.content) is not dict
                or message.content.get("algorithm") != "m.olm.v1.curve25519-aes-sha2"
                or type(message.content.get("sender_key")) is not str
                or type(message.content.get("ciphertext")) is not dict
            ):
                raise ValueError("retained key-share message target is invalid")

    def _save(
        self,
        owner: OwnerView,
        previous: dict[str, _KeyShare],
        entries: dict[str, _KeyShare],
    ) -> bool:
        if previous == entries:
            return False
        for request_id in previous.keys() - entries.keys():
            self.journal._transition_execute(
                "key_share_delete",
                "DELETE FROM NioIngestKeyShare WHERE account_id = ? AND request_id = ?",
                (owner.account_id, request_id),
            )
        for request_id, entry in entries.items():
            if previous.get(request_id) == entry:
                continue
            plain = canonical_json(
                {
                    "request": load_json(entry.request_json, "key request"),
                    "state": entry.state,
                    "message": (
                        None
                        if entry.message_json is None
                        else load_json(entry.message_json, "key-share message")
                    ),
                }
            )
            payload, digest = self.journal._payload(
                owner,
                "NioIngestKeyShare",
                plain,
                header=canonical_json([request_id, owner.revision + 1]),
            )
            if len(payload) > 1024 * 1024:
                raise JournalCapacityError("retained key share exceeds capacity")
            self.journal._transition_execute(
                "key_share_upsert",
                "INSERT INTO NioIngestKeyShare VALUES (?, ?, ?, ?, ?) "
                "ON CONFLICT(account_id, request_id) DO UPDATE SET "
                "updated_revision = excluded.updated_revision, payload = excluded.payload, "
                "payload_sha256 = excluded.payload_sha256",
                (owner.account_id, request_id, owner.revision + 1, payload, digest),
            )
        count, size = self.journal._execute(
            "SELECT COUNT(*), COALESCE(SUM(LENGTH(payload)), 0) FROM NioIngestKeyShare"
        ).fetchone()
        if count > 20_000 or size > 64 * 1024 * 1024:
            raise JournalCapacityError("retained key shares exceed capacity")
        return True

    def restore(self) -> None:
        with self.journal._read():
            entries = self._load(self.journal.load_owner())
        olm = self.client.olm
        assert olm is not None
        olm.key_request_from_untrusted = {
            request_id: entry.request()
            for request_id, entry in entries.items()
            if entry.state == "pending"
        }
        for entry in entries.values():
            if entry.state == "message":
                message = entry.message()
                if message not in olm.outgoing_to_device_messages:
                    olm.outgoing_to_device_messages.append(message)

    def prepare(self) -> dict[str, _KeyShare]:
        entries = self._load(self.journal.load_owner())
        olm = self.client.olm
        assert olm is not None
        for request_id, entry in entries.items():
            if entry.state == "claiming":
                raise JournalIntegrityError(
                    "unsettled interactive claim preceded preparation"
                )
            if entry.state == "waiting":
                olm.received_key_requests[request_id] = entry.request()
            elif entry.state == "message":
                message = entry.message()
                if message not in olm.outgoing_to_device_messages:
                    olm.outgoing_to_device_messages.append(message)
        return entries

    def capture(self, previous: dict[str, _KeyShare]) -> None:
        olm = self.client.olm
        assert olm is not None
        entries = {
            request_id: _KeyShare(canonical_json(event.source))
            for request_id, event in olm.key_request_from_untrusted.items()
        }
        for request_id, entry in previous.items():
            if entry.state != "waiting":
                continue
            request = entry.request()
            target = (request.sender, request.requesting_device_id)
            if request == olm.key_requests_waiting_for_session.get(target, {}).get(
                request_id
            ):
                entries[request_id] = _KeyShare(entry.request_json, "claiming")
        if previous != entries:
            self._save(self.journal.load_owner(), previous, entries)

    def finish_claim(
        self, requests: list[RoomKeyRequest], untrusted: list[RoomKeyRequest]
    ) -> None:
        owner = self.journal.load_owner()
        previous = self._load(owner)
        entries = dict(previous)
        for request in untrusted:
            entry = previous.get(request.request_id)
            if (
                entry is None
                or entry.state != "claiming"
                or entry.request_json != canonical_json(request.source)
            ):
                raise JournalIntegrityError(
                    "waiting key claim generated unsupported callback Work"
                )
        for request in requests:
            entry = previous.get(request.request_id)
            if (
                entry is not None
                and entry.state == "claiming"
                and entry.request_json == canonical_json(request.source)
            ):
                if request in untrusted:
                    entries[request.request_id] = _KeyShare(entry.request_json)
                else:
                    entries.pop(request.request_id)
        self._save(owner, previous, entries)

    def change(self, event: RoomKeyRequest, cancel: bool) -> bool:
        mutation_started = False
        try:
            with self.journal._transaction():
                owner = self.journal.load_owner()
                previous = self._load(owner)
                entries = dict(previous)
                if type(event) is not RoomKeyRequest:
                    raise LocalProtocolError("invalid key share request")
                entry = entries.get(event.request_id)
                if entry is None or entry.request_json != canonical_json(event.source):
                    if cancel:
                        return False
                    raise LocalProtocolError("No such pending key share request found")
                if entry.state != "pending":
                    return not cancel
                olm = self.client.olm
                assert olm is not None
                request = entry.request()
                olm.key_request_from_untrusted[event.request_id] = request
                mutation_started = True
                if cancel:
                    olm.cancel_key_share(request)
                    entries.pop(event.request_id)
                else:
                    before = list(olm.outgoing_to_device_messages)
                    target = (request.sender, request.requesting_device_id)
                    waiting = dict(olm.key_requests_waiting_for_session.get(target, {}))
                    devices = list(olm.key_request_devices_no_session)
                    if not olm.continue_key_share(request):
                        return False
                    messages = olm.outgoing_to_device_messages
                    if (
                        messages[: len(before)] != before
                        or len(messages) > len(before) + 1
                    ):
                        raise JournalIntegrityError(
                            "key-share continuation changed unrelated messages"
                        )
                    if len(messages) > len(before):
                        message = messages[-1]
                        entries[event.request_id] = _KeyShare(
                            entry.request_json,
                            "message",
                            canonical_json(
                                {
                                    "type": message.type,
                                    "recipient": message.recipient,
                                    "recipient_device": message.recipient_device,
                                    "content": message.content,
                                }
                            ),
                        )
                    elif request.request_id in olm.key_requests_waiting_for_session.get(
                        target, {}
                    ):
                        entries[event.request_id] = _KeyShare(
                            entry.request_json, "waiting"
                        )
                    else:
                        entries.pop(event.request_id)
                    # The older Frame owns its frozen waiting buckets until done.
                    olm.key_requests_waiting_for_session[target] = waiting
                    olm.key_request_devices_no_session[:] = devices
                if self._save(owner, previous, entries):
                    updated = self.journal._transition_execute(
                        "meta_revision_epoch_cas",
                        "UPDATE NioIngestMeta SET revision = ? WHERE account_id = ? AND revision = ? AND writer_epoch = ?",
                        (
                            owner.revision + 1,
                            owner.account_id,
                            owner.revision,
                            str(owner.writer_epoch),
                        ),
                    )
                    if updated.rowcount != 1:
                        raise JournalIntegrityError("key-share owner changed")
            return True
        except BaseException:
            if mutation_started:
                self.client._poison_ingestion()
            raise
