"""
Canonical data models for transcript ingestion.
"""

from dataclasses import dataclass, asdict
from typing import Optional
import hashlib


@dataclass
class TranscriptUtterance:
    """
    Normalized transcript utterance in canonical format.
    
    This is the standard format used throughout the pipeline,
    regardless of the upstream source (Vexa, etc.).
    """
    meeting_id: str
    speaker_id: str
    speaker_name: Optional[str]
    text: str
    start_ts: float  # epoch seconds
    end_ts: Optional[float]
    source: str = "vexa"
    
    def to_dict(self) -> dict:
        """Convert to dictionary for storage."""
        return asdict(self)
    
    def compute_id(self) -> str:
        """
        Generate a deterministic ID for deduplication.
        Based on (meeting_id, speaker_id, start_ts, text).
        """
        key = f"{self.meeting_id}:{self.speaker_id}:{self.start_ts}:{self.text}"
        return hashlib.sha256(key.encode()).hexdigest()[:32]
    
    @classmethod
    def from_dict(cls, data: dict) -> "TranscriptUtterance":
        """Reconstruct from dictionary."""
        return cls(
            meeting_id=data["meeting_id"],
            speaker_id=data["speaker_id"],
            speaker_name=data.get("speaker_name"),
            text=data["text"],
            start_ts=data["start_ts"],
            end_ts=data.get("end_ts"),
            source=data.get("source", "vexa"),
        )
