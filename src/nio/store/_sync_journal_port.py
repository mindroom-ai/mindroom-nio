from __future__ import annotations

from typing import Protocol, runtime_checkable
from uuid import UUID

from ..ingest.effects import (
    MembershipOperationRef,
    MembershipOperationResolution,
    MembershipOperationResolutionOutcome,
    MembershipOperationStatus,
    MembershipRequest,
    PersistedNetworkEffect,
)
from ..ingest.model import BatchRef, SyncBatch
from ..ingest.state import (
    AckOutcome,
    CommitResult,
    JournalTransition,
    LaneRecord,
    LaneRecordKey,
    LaneRecordSection,
    OwnerView,
    ReadyRecord,
    RoomAggregate,
    SourceState,
    StagedFrame,
)


@runtime_checkable
class IngestionJournal(Protocol):
    """Dependency-light durable journal port for ingestion actors and fakes."""

    def load_owner(self) -> OwnerView: ...

    def load_source(self) -> SourceState: ...

    def load_rooms(
        self,
        room_ids: frozenset[str],
    ) -> dict[str, RoomAggregate]: ...

    def load_ready_heads(self, limit: int) -> tuple[ReadyRecord, ...]: ...

    def load_lane_record(self, key: LaneRecordKey) -> LaneRecord | None: ...

    def list_lane_records(
        self,
        room_id: str,
        membership_epoch: int,
        section: LaneRecordSection | None = None,
    ) -> tuple[LaneRecord, ...]: ...

    def load_frame(self, frame_id: UUID) -> StagedFrame | None: ...

    def list_frames(self, limit: int) -> tuple[StagedFrame, ...]: ...

    def load_network_effect(
        self,
        effect_id: UUID,
    ) -> PersistedNetworkEffect | None: ...

    def list_network_effects(
        self,
        limit: int,
    ) -> tuple[PersistedNetworkEffect, ...]: ...

    def list_schedulable_network_effects(
        self,
        limit: int,
    ) -> tuple[PersistedNetworkEffect, ...]: ...

    def claim_membership_operation(
        self,
        effect_id: UUID,
    ) -> tuple[MembershipRequest, MembershipOperationRef]: ...

    def uncertain_membership_operations(
        self,
        limit: int,
        *,
        after_effect_id: UUID | None = None,
    ) -> tuple[MembershipOperationStatus, ...]: ...

    def resolve_membership_operation(
        self,
        ref: MembershipOperationRef,
        resolution: MembershipOperationResolution,
    ) -> MembershipOperationResolutionOutcome: ...

    def commit(
        self,
        *,
        expected_revision: int,
        writer_epoch: UUID,
        transition: JournalTransition,
    ) -> CommitResult: ...

    def oldest_unacknowledged(self) -> SyncBatch | None: ...

    def acknowledge(self, ref: BatchRef) -> AckOutcome: ...
