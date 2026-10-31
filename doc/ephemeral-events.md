# Encrypted room payloads and ephemeral events

`AsyncClient.room_encrypt` Megolm-encrypts a room event without sending it.
It prepares the room's group session exactly as `room_send` does: it syncs the members, queries their keys, and shares the session first when it is missing, unshared, or expired.
The encrypted plaintext is exactly `{"type": event_type, "content": content, "room_id": room_id}`, and the method returns the `m.megolm.v1.aes-sha2` content.
Unlike `room_send`, it does not copy `m.relates_to` to the unencrypted content.
It raises `LocalProtocolError` when encryption is not enabled or the room is unknown or not encrypted, and with `DurableSync` it applies the same room readiness gate as `room_send`.

`AsyncClient.room_send_ephemeral` sends an [MSC2477](https://github.com/matrix-org/matrix-spec-proposals/pull/2477) ephemeral event with `PUT /_matrix/client/unstable/org.matrix.msc2477/rooms/{roomId}/ephemeral/{eventType}/{txnId}`.
The server relays the event to the room's members through sync without storing it in the room.
The content is sent as given, so an application that sends to an encrypted room wraps the result of `room_encrypt` in its own content.

```python
encrypted = await client.room_encrypt(
    room_id,
    "com.example.live",
    {"seq": 3, "text": "partial reply"},
    count_toward_rotation=False,
)
response = await client.room_send_ephemeral(
    room_id, "com.example.live", {"v": 1, "encrypted": encrypted}
)
```

The result is a `RoomSendEphemeralResponse` or a `RoomSendEphemeralError`.
With the default `AsyncClientConfig`, a `429` response is retried after its `retry_after_ms` with the same transaction ID, like every other request.
Set `max_limit_exceeded=0` to receive the `RoomSendEphemeralError` with its `retry_after_ms` instead.

## Rotation exemption

An outbound group session rotates after 100 counted messages or 7 days, and when room membership or a member's devices change.
`count_toward_rotation=False` exempts a `room_encrypt` call from the 100-message limit only.
Exempt encryptions still advance the Megolm message index, so one session covers more ciphertexts, and a device holding the session key at some index can decrypt every later one.
Age and membership rotation still apply, and a session that has expired for any reason is rotated and shared before the next encryption.
An exempt message's content need not equal any durable room event: a device that receives the exempt ciphertext can read intermediate content that the room's history never records.
