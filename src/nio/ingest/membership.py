from dataclasses import dataclass
from enum import StrEnum


def _require_exact(value: object, expected: type, field_name: str) -> None:
    if type(value) is not expected:
        raise TypeError(f"{field_name} must be {expected.__name__}")


def _require_optional_string(value: object, field_name: str) -> None:
    if value is not None:
        _require_exact(value, str, field_name)


@dataclass(frozen=True, slots=True)
class MembershipBaseline:
    prev_batch: str
    membership_event_id: str

    def __post_init__(self) -> None:
        _require_exact(self.prev_batch, str, "prev_batch")
        _require_exact(self.membership_event_id, str, "membership_event_id")
        if not self.prev_batch or not self.membership_event_id:
            raise ValueError("membership baseline values must not be empty")


@dataclass(frozen=True, slots=True)
class MembershipObservation:
    room_membership: str | None
    event_membership: str | None
    event_id: str | None
    previous_membership: str | None
    replaces_state: str | None
    is_live: bool
    is_initial: bool
    is_expanded_timeline: bool
    is_unparsed: bool

    def __post_init__(self) -> None:
        allowed = {None, "join", "invite", "knock", "leave", "ban"}
        _require_optional_string(self.room_membership, "room_membership")
        _require_optional_string(self.event_membership, "event_membership")
        if self.room_membership not in allowed:
            raise ValueError("unknown room membership")
        if self.event_membership not in allowed:
            raise ValueError("unknown membership event value")
        _require_optional_string(self.event_id, "event_id")
        _require_optional_string(self.previous_membership, "previous_membership")
        _require_optional_string(self.replaces_state, "replaces_state")
        _require_exact(self.is_live, bool, "is_live")
        _require_exact(self.is_initial, bool, "is_initial")
        _require_exact(
            self.is_expanded_timeline,
            bool,
            "is_expanded_timeline",
        )
        _require_exact(self.is_unparsed, bool, "is_unparsed")


class MembershipProofKind(StrEnum):
    CONTINUES = "continues"
    ESTABLISHED = "established"
    CHANGED = "changed"
    DEPARTED = "departed"
    UNSAFE = "unsafe"


@dataclass(frozen=True, slots=True)
class MembershipProof:
    kind: MembershipProofKind
    membership_event_id: str | None

    def __post_init__(self) -> None:
        _require_exact(self.kind, MembershipProofKind, "kind")
        _require_optional_string(self.membership_event_id, "membership_event_id")
        proves_join = self.kind in {
            MembershipProofKind.CONTINUES,
            MembershipProofKind.ESTABLISHED,
            MembershipProofKind.CHANGED,
        }
        if proves_join != bool(self.membership_event_id):
            raise ValueError("membership proof kind contradicts its event identity")


def prove_membership(
    baseline: MembershipBaseline | None,
    observation: MembershipObservation,
) -> MembershipProof:
    if baseline is not None and type(baseline) is not MembershipBaseline:
        raise TypeError("baseline must be MembershipBaseline or None")
    _require_exact(observation, MembershipObservation, "observation")

    unsafe = MembershipProof(MembershipProofKind.UNSAFE, None)
    departed = MembershipProof(MembershipProofKind.DEPARTED, None)
    if observation.room_membership in {"invite", "knock", "leave", "ban"}:
        return departed
    if observation.is_unparsed:
        return unsafe
    if observation.event_membership in {"invite", "knock", "leave", "ban"}:
        return departed

    if observation.event_membership == "join":
        event_id = observation.event_id
        if not event_id:
            return unsafe
        if baseline is None:
            return MembershipProof(MembershipProofKind.ESTABLISHED, event_id)
        if event_id == baseline.membership_event_id:
            return MembershipProof(MembershipProofKind.CONTINUES, event_id)
        if observation.is_live:
            return MembershipProof(MembershipProofKind.CHANGED, event_id)
        if (
            observation.previous_membership == "join"
            and observation.replaces_state == baseline.membership_event_id
        ):
            return MembershipProof(MembershipProofKind.CONTINUES, event_id)
        return unsafe

    if observation.is_initial or observation.is_expanded_timeline or baseline is None:
        return unsafe
    return MembershipProof(
        MembershipProofKind.CONTINUES,
        baseline.membership_event_id,
    )


def membership_recovery_cursor(
    baseline: MembershipBaseline | None,
    proof: MembershipProof,
    *,
    discontinuity: bool,
    current_prev_batch: str | None,
) -> str | None:
    if baseline is not None and type(baseline) is not MembershipBaseline:
        raise TypeError("baseline must be MembershipBaseline or None")
    _require_exact(proof, MembershipProof, "proof")
    _require_exact(discontinuity, bool, "discontinuity")
    _require_optional_string(current_prev_batch, "current_prev_batch")
    if (
        baseline is None
        or proof.kind is not MembershipProofKind.CONTINUES
        or not discontinuity
        or not current_prev_batch
    ):
        return None
    return baseline.prev_batch


def advance_membership_baseline(
    baseline: MembershipBaseline | None,
    proof: MembershipProof,
    *,
    current_prev_batch: str | None,
) -> MembershipBaseline | None:
    if baseline is not None and type(baseline) is not MembershipBaseline:
        raise TypeError("baseline must be MembershipBaseline or None")
    _require_exact(proof, MembershipProof, "proof")
    _require_optional_string(current_prev_batch, "current_prev_batch")
    if proof.kind in {MembershipProofKind.DEPARTED, MembershipProofKind.UNSAFE}:
        return None
    assert proof.membership_event_id is not None
    if current_prev_batch:
        return MembershipBaseline(current_prev_batch, proof.membership_event_id)
    if proof.kind is MembershipProofKind.CONTINUES and baseline is not None:
        return MembershipBaseline(baseline.prev_batch, proof.membership_event_id)
    return None
