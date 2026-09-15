# Authenticated custom to-device transport

`AsyncClient.encrypted_to_device` sends a custom event to one exact Matrix device using Olm.
The caller supplies the recipient's pinned Ed25519 key.
Nio queries fresh signed device keys, rejects a missing, deleted, blacklisted, or mismatched device, and claims an Olm key when needed.
The pin authorizes this send without marking the device globally verified.

```python
from nio import ToDeviceMessage

response = await client.encrypted_to_device(
    ToDeviceMessage(
        "com.example.command",
        "@recipient:example.org",
        "RECIPIENT_DEVICE",
        {"request_id": "unique-application-request", "action": "status"},
    ),
    recipient_ed25519=recipient_fingerprint,
)
```

`await client.keys_query({"@recipient:example.org"})` explicitly discovers that user's full device list, even without a shared encrypted room.
Omitting the argument preserves normal dirty-device-list refresh behavior.
Exact-device filtering is intentionally not used: a complete user device list is needed to identify deleted siblings correctly.

## Ownership and retries

With `DurableSync`, encryption, Olm ratchet persistence, and the exact outgoing HTTP request commit in one transaction.
Cancellation or restart retries that retained ciphertext and Matrix transaction ID.
The application must separately persist admission, execution state, and outcomes for its own commands.
Nio's transport delivery is not proof that a remote application executed a command.

An explicit `tx_id` identifies a pending transport operation.
Changing the payload or pin while reusing that pending ID raises `LocalProtocolError`.
Completed IDs are not retained; use a fresh ID for a new delivery attempt after success, and deduplicate application requests independently.
Omitting `tx_id` lets Nio choose IDs and recognize an identical pending operation.

Ordinary clients use the same validation and encryption path, but retain failed ciphertext only in memory.
They do not provide crash-durable application sends.
Durable HTTP errors raise typed exceptions from `nio.durable.transport`; ordinary requests can also return `ToDeviceError`.

## Authenticated receive evidence

Successfully decrypted custom events with a unique signed sender-device match become `AuthenticatedToDeviceEvent`.
Its immutable `authenticated_sender` contains the Matrix user, device ID, Curve25519 key, and Ed25519 key authenticated at decryption time.
Plaintext event parsing never constructs this evidence.
Events without a proven identity remain ordinary unknown events and must not authorize application actions.

Durable records retain this evidence alongside their original encrypted envelope and replay it without substituting a newly discovered identity.
Historical authentication is separate from current authority.
Applications must still check current deletion/blacklist state, pinned keys, requester policy, expiry, and local permissions before acting.

## To-device-only sources

Use `DurableSyncConfig(to_device_only=True)` for a dedicated device-command consumer.
This mode keeps room, presence, and global account-data sections outside the source's ownership while retaining to-device input and crypto maintenance.
Its fixed classic-sync filter stays narrow after cursor advancement and restart; room recovery cannot widen it.
A custom filter, Sliding Sync configuration, or local room membership operation is rejected in this mode.

The mode is bound to the durable stream.
An existing room-owning stream cannot be reopened as to-device-only, or vice versa.
Use the same persisted consumer identity and store for restart; do not discard pending input to change modes.
Default durable sources retain their existing room recovery behavior.
