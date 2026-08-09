from __future__ import annotations

import hashlib
import hmac
import json
import os
from uuid import UUID

from Crypto.Cipher import AES

from ..ingest.errors import JournalIntegrityError
from .sync_journal_schema import SCHEMA_VERSION

_KEY_DOMAIN = b"mindroom-nio:ingest-row-key:v1\0"
_AAD_DOMAIN = b"mindroom-nio:ingest-row-aad:v1\0"
_NONCE_SIZE = 12
_TAG_SIZE = 16


def _length_frame(value: bytes) -> bytes:
    return len(value).to_bytes(4, "big") + value


class EncryptedRowCodec:
    """AES-GCM codec bound to schema, table, owner, stream, key, and digest."""

    def __init__(self, pickle_key: str, account_id: str, stream_id: UUID) -> None:
        if type(pickle_key) is not str:
            raise TypeError("pickle_key must be str")
        if type(account_id) is not str:
            raise TypeError("account_id must be str")
        if type(stream_id) is not UUID:
            raise TypeError("stream_id must be UUID")
        self._key = hashlib.sha256(_KEY_DOMAIN + pickle_key.encode()).digest()
        self.account_id = account_id
        self.stream_id = stream_id

    @staticmethod
    def _primary_key_payload(primary_key: tuple[str | int | UUID, ...]) -> bytes:
        if type(primary_key) is not tuple:
            raise TypeError("primary_key must be a tuple")
        if any(type(value) not in (str, int, UUID) for value in primary_key):
            raise TypeError("primary_key values must be str, int, or UUID")
        values = [str(value) if type(value) is UUID else value for value in primary_key]
        return json.dumps(values, separators=(",", ":"), ensure_ascii=False).encode()

    def _aad(
        self,
        table: str,
        primary_key: tuple[str | int | UUID, ...],
        digest: bytes,
        header: bytes,
    ) -> bytes:
        if type(table) is not str or not table:
            raise TypeError("table must be a nonempty str")
        if type(digest) is not bytes or len(digest) != hashlib.sha256().digest_size:
            raise TypeError("digest must be a SHA-256 bytes value")
        if type(header) is not bytes:
            raise TypeError("header must be bytes")
        fields = (
            str(SCHEMA_VERSION).encode(),
            table.encode(),
            self.account_id.encode(),
            str(self.stream_id).encode(),
            self._primary_key_payload(primary_key),
            digest,
            header,
        )
        return _AAD_DOMAIN + b"".join(_length_frame(value) for value in fields)

    def seal(
        self,
        table: str,
        primary_key: tuple[str | int | UUID, ...],
        payload: bytes,
        header: bytes = b"",
    ) -> tuple[bytes, bytes]:
        digest = hashlib.sha256(payload).digest()
        return self.encrypt(table, primary_key, payload, digest, header), digest

    def encrypt(
        self,
        table: str,
        primary_key: tuple[str | int | UUID, ...],
        payload: bytes,
        digest: bytes | None = None,
        header: bytes = b"",
    ) -> bytes:
        if type(payload) is not bytes:
            raise TypeError("payload must be bytes")
        actual_digest = hashlib.sha256(payload).digest()
        digest = actual_digest if digest is None else digest
        if not hmac.compare_digest(actual_digest, digest):
            raise JournalIntegrityError("payload digest does not match plaintext")
        nonce = os.urandom(_NONCE_SIZE)
        cipher = AES.new(self._key, AES.MODE_GCM, nonce=nonce, mac_len=_TAG_SIZE)
        cipher.update(self._aad(table, primary_key, digest, header))
        encrypted, tag = cipher.encrypt_and_digest(payload)
        return bytes((SCHEMA_VERSION,)) + nonce + tag + encrypted

    def decrypt(
        self,
        table: str,
        primary_key: tuple[str | int | UUID, ...],
        ciphertext: bytes,
        digest: bytes,
        header: bytes = b"",
    ) -> bytes:
        if (
            type(ciphertext) is not bytes
            or len(ciphertext) < 1 + _NONCE_SIZE + _TAG_SIZE
            or ciphertext[0] != SCHEMA_VERSION
        ):
            raise JournalIntegrityError("invalid encrypted ingestion row")
        nonce_end, tag_end = 1 + _NONCE_SIZE, 1 + _NONCE_SIZE + _TAG_SIZE
        cipher = AES.new(
            self._key,
            AES.MODE_GCM,
            nonce=ciphertext[1:nonce_end],
            mac_len=_TAG_SIZE,
        )
        try:
            cipher.update(self._aad(table, primary_key, digest, header))
            payload = cipher.decrypt_and_verify(
                ciphertext[tag_end:], ciphertext[nonce_end:tag_end]
            )
        except (TypeError, ValueError) as error:
            raise JournalIntegrityError(
                "ingestion row authentication failed"
            ) from error
        if not hmac.compare_digest(hashlib.sha256(payload).digest(), digest):
            raise JournalIntegrityError("ingestion row digest mismatch")
        return payload
