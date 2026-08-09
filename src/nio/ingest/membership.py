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
    room_id: str
    source_epoch: int
    membership_epoch: int
    prev_batch: str
    membership_event_id: str

    def __post_init__(self) -> None:
        for name in ("room_id", "prev_batch", "membership_event_id"):
            _require_exact(getattr(self, name), str, name)
        for name in ("source_epoch", "membership_epoch"):
            value = getattr(self, name)
            _require_exact(value, int, name)
            if value < 0:
                raise ValueError(f"{name} must be nonnegative")
        if not self.room_id or not self.prev_batch or not self.membership_event_id:
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
        if observation.is_live:
            return MembershipProof(MembershipProofKind.CHANGED, event_id)
        if event_id == baseline.membership_event_id:
            return MembershipProof(MembershipProofKind.CONTINUES, event_id)
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
    room_id: str,
    source_epoch: int,
    membership_epoch: int,
    current_prev_batch: str | None,
) -> MembershipBaseline | None:
    if baseline is not None and type(baseline) is not MembershipBaseline:
        raise TypeError("baseline must be MembershipBaseline or None")
    _require_exact(proof, MembershipProof, "proof")
    _require_exact(room_id, str, "room_id")
    _require_exact(source_epoch, int, "source_epoch")
    _require_exact(membership_epoch, int, "membership_epoch")
    _require_optional_string(current_prev_batch, "current_prev_batch")
    if not room_id:
        raise ValueError("room_id must not be empty")
    if source_epoch < 0 or membership_epoch < 0:
        raise ValueError("source_epoch and membership_epoch must be nonnegative")
    if proof.kind in {MembershipProofKind.DEPARTED, MembershipProofKind.UNSAFE}:
        return None
    assert proof.membership_event_id is not None
    if current_prev_batch:
        return MembershipBaseline(
            room_id,
            source_epoch,
            membership_epoch,
            current_prev_batch,
            proof.membership_event_id,
        )
    if proof.kind is MembershipProofKind.CONTINUES and baseline is not None:
        if baseline.room_id != room_id or baseline.membership_epoch != membership_epoch:
            raise ValueError("baseline room/epoch does not match durable context")
        return MembershipBaseline(
            baseline.room_id,
            baseline.source_epoch,
            baseline.membership_epoch,
            baseline.prev_batch,
            proof.membership_event_id,
        )
    return None
