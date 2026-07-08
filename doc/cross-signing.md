# Cross-signing (fork feature) — scope and roadmap

This fork adds a minimal, self-managed cross-signing implementation aimed at
bot-style clients (see `AsyncClient.ensure_cross_signing` and
`nio/crypto/cross_signing.py`).
Upstream matrix-nio has never implemented cross-signing, so this is a
fork-specific addition rather than a backport.

## What is implemented

The producer side of the official Matrix cross-signing spec, scoped to what a
bot needs to remain usable under MSC4153 ("exclude non-cross-signed devices"):

- Generate a master key and a self-signing key (Ed25519), persisted next to the
  encryption store in a `0600` sidecar.
- Sign the self-signing key with the master key.
- Sign the account's own device with the self-signing key, uploading the
  device's own keys first when they are not yet on the server.
- Upload via the standard endpoints `POST /_matrix/client/v3/keys/device_signing/upload`
  (with an MSC3967-first flow and a password-based user-interactive-auth retry)
  and `POST /_matrix/client/v3/keys/signatures/upload`.
- Signing uses the standard Matrix algorithm: Ed25519 over canonical JSON with
  `signatures`/`unsigned` removed, unpadded base64 — the same scheme nio already
  uses for device keys.

This is sufficient for MindRoom's use case: agent bot devices present a
cross-signed identity, so clients that adopt MSC4153 keep sharing room keys with
them.

## Deliberately out of scope (bot simplifications)

Both are permitted by the spec ("MAY"):

- No user-signing key is created; bots never verify other users, and omitting it
  keeps the cross-user trust surface out of the fork.
- The device does not additionally sign the master key; this only affects how
  other clients render the bot's identity and is not required for the
  device → self-signing → master chain that MSC4153 checks.

## Not yet implemented (needed for general, human-facing parity)

A general soft fork aiming at parity with the reference client
(matrix-rust-sdk) would still need the following.
These are not required for the bot use case and are large enough that upstream
contribution may be preferable to carrying them as a permanent fork delta.

1. Consumer/verifier side (largest gap): parse `master_keys`,
   `self_signing_keys`, and `user_signing_keys` from `/keys/query`, persist
   them, and compute device/user trust by verifying the
   device → self-signing → master signature chain, wiring the result into
   `TrustState`/`is_device_verified` so the client can enforce MSC4153 on
   messages it receives. Rough estimate: about one week including a store schema
   addition and tests.
2. Secret Storage (SSSS / "4S", `m.secret_storage` plus the
   `m.secret.request`/`m.secret.send` gossip): store the cross-signing private
   keys encrypted on the server under a recovery key and fetch them on a new
   device, so cross-signing survives device loss without regenerating keys.
   Rough estimate: one to two weeks.
3. Server-side key backup (`m.megolm_backup.v1.curve25519-aes-sha2`): back up
   and restore room keys. Independent of cross-signing but part of full E2EE.
   Rough estimate: about one week.
4. User-signing key plus a SAS-to-cross-signing tie-in for cross-user
   verification. Small once item 1 exists; low value for bots.
   Rough estimate: one to two days.

One cheap correctness improvement independent of the above: have the device also
sign the master key on upload (about ten lines), which improves identity
rendering in other clients.

Full parity is realistically three to five weeks of focused work plus interop
testing against Synapse and Element.
For this fork's stated purpose, item 1 (the verifier side) is the highest-value
next step and the natural follow-up.
