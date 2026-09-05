from __future__ import annotations

from typing import Protocol, runtime_checkable
from uuid import UUID

from ..ingest.hydration import HydrationResult, PendingHydration
from ..ingest.model import BatchRef, SyncBatch
from ..ingest.state import CommitResult, OwnerView, SourceState, StagedFrame


@runtime_checkable
class IngestionJournal(Protocol):
    """Source-only durable journal port for ingestion actors and fakes."""

    def load_owner(self) -> OwnerView: ...

    def next_batch(
        self,
        *,
        max_records: int = 256,
        max_canonical_bytes: int = 16 * 1024 * 1024,
    ) -> SyncBatch | None: ...

    def acknowledge_batch(self, ref: BatchRef) -> None: ...

    def load_source(self) -> SourceState: ...

    def load_frame(self, frame_id: UUID) -> StagedFrame | None: ...

    def list_frames(self, limit: int) -> tuple[StagedFrame, ...]: ...

    def has_reserved_quiesce_response(self) -> bool: ...

    def consume_reserved_quiesce_response(self) -> CommitResult | None: ...

    def stage_source_response(
        self,
        *,
        source: SourceState,
        frame: StagedFrame,
        quiesce_reserved: bool = False,
    ) -> CommitResult: ...

    def load_pending_hydrations(
        self, *, limit: int
    ) -> tuple[PendingHydration, ...]: ...

    def apply_hydration_result(
        self, *, result: HydrationResult
    ) -> CommitResult | None: ...
