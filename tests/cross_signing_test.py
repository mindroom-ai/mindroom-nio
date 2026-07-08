"""Tests for the fork's self-managed cross-signing identities."""

import pytest
import vodozemac

from nio.api import Api
from nio.crypto.cross_signing import (
    CrossSigningIdentity,
    cross_signing_sidecar_path,
    sign_json,
)

ALICE = "@alice:example.org"


def _verify(public_key_b64, json_dict, signature_b64):
    unsigned = {
        key: value
        for key, value in json_dict.items()
        if key not in ("signatures", "unsigned")
    }
    message = Api.to_canonical_json(unsigned).encode("utf-8")
    public_key = vodozemac.Ed25519PublicKey.from_base64(public_key_b64)
    signature = vodozemac.Ed25519Signature.from_base64(signature_b64)
    public_key.verify_signature(message, signature)


class TestCrossSigning:
    def test_master_key_payload_shape(self):
        identity = CrossSigningIdentity.generate(ALICE)
        payload = identity.master_key_payload()

        assert payload["user_id"] == ALICE
        assert payload["usage"] == ["master"]
        assert payload["keys"] == {
            f"ed25519:{identity.master_public_key}": identity.master_public_key
        }

    def test_self_signing_key_is_signed_by_master(self):
        identity = CrossSigningIdentity.generate(ALICE)
        payload = identity.self_signing_key_payload()

        assert payload["usage"] == ["self_signing"]
        signature = payload["signatures"][ALICE][
            f"ed25519:{identity.master_public_key}"
        ]
        _verify(identity.master_public_key, payload, signature)

    def test_device_signature_verifies_with_self_signing_key(self):
        identity = CrossSigningIdentity.generate(ALICE)
        device_keys = {
            "algorithms": ["m.olm.v1.curve25519-aes-sha2", "m.megolm.v1.aes-sha2"],
            "device_id": "DEVICEID",
            "user_id": ALICE,
            "keys": {"curve25519:DEVICEID": "curve", "ed25519:DEVICEID": "ed"},
            "signatures": {ALICE: {"ed25519:DEVICEID": "own-device-signature"}},
        }

        signed = identity.signed_device_payload(device_keys)

        assert "unsigned" not in signed
        signature = signed["signatures"][ALICE][
            f"ed25519:{identity.self_signing_public_key}"
        ]
        _verify(identity.self_signing_public_key, device_keys, signature)

    def test_signature_excludes_signatures_and_unsigned(self):
        identity = CrossSigningIdentity.generate(ALICE)
        bare = {"user_id": ALICE, "usage": ["master"]}
        decorated = dict(bare, signatures={"x": {}}, unsigned={"y": 1})

        assert sign_json(identity.master_seed, bare) == sign_json(
            identity.master_seed, decorated
        )

    def test_persistence_roundtrip(self, tmp_path):
        identity = CrossSigningIdentity.generate(ALICE)
        identity.uploaded = True
        identity.signed_devices = ["DEVICEID"]
        sidecar = cross_signing_sidecar_path(str(tmp_path), ALICE)

        identity.save(sidecar)
        loaded = CrossSigningIdentity.load(sidecar)

        assert loaded is not None
        assert loaded.user_id == ALICE
        assert loaded.master_seed == identity.master_seed
        assert loaded.self_signing_seed == identity.self_signing_seed
        assert loaded.uploaded is True
        assert loaded.signed_devices == ["DEVICEID"]
        assert loaded.master_public_key == identity.master_public_key

    def test_load_missing_returns_none(self, tmp_path):
        sidecar = cross_signing_sidecar_path(str(tmp_path), ALICE)
        assert CrossSigningIdentity.load(sidecar) is None

    def test_load_corrupt_raises_instead_of_rotating(self, tmp_path):
        # A corrupt existing sidecar must not look "absent"; returning None
        # would make ensure_cross_signing rotate keys and break signatures.
        sidecar = cross_signing_sidecar_path(str(tmp_path), ALICE)
        sidecar.parent.mkdir(parents=True, exist_ok=True)
        sidecar.write_text("not json")
        with pytest.raises(ValueError):  # noqa: PT011
            CrossSigningIdentity.load(sidecar)

    def test_save_creates_owner_only_file(self, tmp_path):
        identity = CrossSigningIdentity.generate(ALICE)
        sidecar = cross_signing_sidecar_path(str(tmp_path), ALICE)

        identity.save(sidecar)

        assert (sidecar.stat().st_mode & 0o777) == 0o600
