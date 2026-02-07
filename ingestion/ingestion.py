"""
Ingestion pipeline for real-time transcript processing.

Coordinates polling from Vexa and storage into Qdrant.
"""

import logging
import signal
import time
from typing import Callable, Optional, Set

from .models import TranscriptUtterance
from .vexa_client import VexaClient
from .qdrant_store import QdrantStore

logger = logging.getLogger(__name__)


class IngestionPipeline:
    """
    Real-time transcript ingestion pipeline.
    
    Polls Vexa for new transcript data and stores it in Qdrant.
    Handles deduplication and graceful shutdown.
    """
    
    def __init__(
        self,
        vexa_client: VexaClient,
        qdrant_store: QdrantStore,
        meeting_id: str,
        poll_interval: float = 2.0,
        on_new_utterance: Optional[Callable[[TranscriptUtterance], None]] = None,
    ):
        """
        Initialize the ingestion pipeline.
        
        Args:
            vexa_client: Client for fetching transcripts from Vexa
            qdrant_store: Store for persisting utterances
            meeting_id: The meeting ID to monitor
            poll_interval: Seconds between polls (default: 2.0)
            on_new_utterance: Optional callback for each new utterance.
                              Hook point for downstream processing.
        """
        self.vexa_client = vexa_client
        self.qdrant_store = qdrant_store
        self.meeting_id = meeting_id
        self.poll_interval = poll_interval
        self.on_new_utterance = on_new_utterance
        
        # State tracking
        self._running = False
        self._last_ts: Optional[float] = None
        self._seen_ids: Set[str] = set()
        self._stats = {
            "polls": 0,
            "utterances_ingested": 0,
            "duplicates_skipped": 0,
            "errors": 0,
        }
        
        logger.info(
            f"IngestionPipeline initialized: meeting={meeting_id}, "
            f"poll_interval={poll_interval}s"
        )
    
    def run(self, max_iterations: Optional[int] = None):
        """
        Start the ingestion loop.
        
        Args:
            max_iterations: Optional limit on number of poll cycles.
                           None = run indefinitely until stopped.
        """
        self._running = True
        self._setup_signal_handlers()
        
        logger.info(f"Starting ingestion loop for meeting: {self.meeting_id}")
        iteration = 0
        
        while self._running:
            try:
                self._poll_and_ingest()
                iteration += 1
                
                if max_iterations and iteration >= max_iterations:
                    logger.info(f"Reached max iterations ({max_iterations}), stopping")
                    break
                
                time.sleep(self.poll_interval)
                
            except KeyboardInterrupt:
                logger.info("Received keyboard interrupt")
                break
            except Exception as e:
                logger.error(f"Error in ingestion loop: {e}")
                self._stats["errors"] += 1
                time.sleep(self.poll_interval)
        
        self._running = False
        self._log_final_stats()
    
    def stop(self):
        """Signal the pipeline to stop gracefully."""
        logger.info("Stopping ingestion pipeline...")
        self._running = False
    
    def _poll_and_ingest(self):
        """Single poll cycle: fetch new utterances and store them."""
        self._stats["polls"] += 1
        
        # Fetch new utterances from Vexa
        try:
            utterances = self.vexa_client.fetch_latest_transcript(
                meeting_id=self.meeting_id,
                since_ts=self._last_ts,
            )
        except Exception as e:
            logger.error(f"Failed to fetch transcript: {e}")
            self._stats["errors"] += 1
            return
        
        if not utterances:
            return
        
        # Deduplicate
        new_utterances = []
        for utterance in utterances:
            uid = utterance.compute_id()
            if uid not in self._seen_ids:
                new_utterances.append(utterance)
                self._seen_ids.add(uid)
            else:
                self._stats["duplicates_skipped"] += 1
        
        if not new_utterances:
            return
        
        # Store in Qdrant
        success = self.qdrant_store.upsert_utterances(new_utterances)
        
        if success:
            for utterance in new_utterances:
                self._stats["utterances_ingested"] += 1
                logger.info(
                    f"Ingested: [{utterance.speaker_name or utterance.speaker_id}] "
                    f"\"{utterance.text[:50]}{'...' if len(utterance.text) > 50 else ''}\""
                )
                
                # Call downstream hook if registered
                if self.on_new_utterance:
                    try:
                        self.on_new_utterance(utterance)
                    except Exception as e:
                        logger.error(f"Error in on_new_utterance callback: {e}")
            
            # Update timestamp for next poll
            max_ts = max(u.start_ts for u in new_utterances)
            if self._last_ts is None or max_ts > self._last_ts:
                self._last_ts = max_ts
        else:
            logger.error("Failed to store utterances in Qdrant")
            self._stats["errors"] += 1
    
    def _setup_signal_handlers(self):
        """Set up handlers for graceful shutdown."""
        def handle_signal(signum, frame):
            logger.info(f"Received signal {signum}")
            self.stop()
        
        signal.signal(signal.SIGTERM, handle_signal)
        signal.signal(signal.SIGINT, handle_signal)
    
    def _log_final_stats(self):
        """Log final statistics when pipeline stops."""
        logger.info(
            f"Ingestion pipeline stopped. Stats: "
            f"polls={self._stats['polls']}, "
            f"ingested={self._stats['utterances_ingested']}, "
            f"duplicates_skipped={self._stats['duplicates_skipped']}, "
            f"errors={self._stats['errors']}"
        )
    
    @property
    def stats(self) -> dict:
        """Get current pipeline statistics."""
        return self._stats.copy()
    
    @property
    def is_running(self) -> bool:
        """Check if the pipeline is currently running."""
        return self._running


class BatchIngestionPipeline(IngestionPipeline):
    """
    Variant that processes utterances in batches.
    
    Useful for higher throughput scenarios where individual
    callbacks per utterance would be too slow.
    
    TODO: Implement if needed for future streaming mode.
    """
    pass
