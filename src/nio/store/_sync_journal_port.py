from __future__ import annotations

from typing import Protocol, runtime_checkable
from uuid import UUID

from ..ingest.state import CommitResult, OwnerView, SourceState, StagedFrame
from ._sync_journal_values import MaterializeResult, MaterializerLimits


@runtime_checkable
class IngestionJournal(Protocol):
    """Source-only durable journal port for ingestion actors and fakes."""

    def load_owner(self) -> OwnerView: ...

    def load_source(self) -> SourceState: ...

    def load_frame(self, frame_id: UUID) -> StagedFrame | None: ...

    def list_frames(self, limit: int) -> tuple[StagedFrame, ...]: ...

    def stage_source_response(
        self,
        *,
        source: SourceState,
        frame: StagedFrame,
    ) -> CommitResult: ...

    def materialize_oldest_frame(
        self,
        *,
        limits: MaterializerLimits,
    ) -> MaterializeResult: ...

    def materialize_oldest_diagnostic_frame(
        self,
        *,
        room_id: str,
        limits: MaterializerLimits = MaterializerLimits(),
    ) -> MaterializeResult: ...
