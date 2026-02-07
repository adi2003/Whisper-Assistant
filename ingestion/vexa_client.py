"""
Vexa Meeting Bot API client.

Handles authentication and transcript fetching from Vexa.
Supports incremental fetching to avoid duplicate utterances.
"""

import logging
import time
from typing import Optional
import requests

from .models import TranscriptUtterance

logger = logging.getLogger(__name__)


class VexaClient:
    """
    Client for the Vexa Meeting Bot API.
    
    Fetches real-time transcript data from active meetings.
    """
    
    # TODO: Update with actual Vexa API base URL
    DEFAULT_BASE_URL = "https://api.vexa.ai/v1"
    
    def __init__(
        self,
        api_key: str,
        base_url: Optional[str] = None,
        timeout: float = 10.0,
    ):
        """
        Initialize Vexa client.
        
        Args:
            api_key: Vexa API key for authentication
            base_url: Optional custom API base URL
            timeout: Request timeout in seconds
        """
        self.api_key = api_key
        self.base_url = (base_url or self.DEFAULT_BASE_URL).rstrip("/")
        self.timeout = timeout
        self._session = requests.Session()
        self._session.headers.update({
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            # TODO: Confirm header format with Vexa API docs
        })
        logger.info(f"VexaClient initialized, base_url={self.base_url}")
    
    def fetch_latest_transcript(
        self,
        meeting_id: str,
        since_ts: Optional[float] = None,
    ) -> list[TranscriptUtterance]:
        """
        Fetch transcript segments from a meeting.
        
        Args:
            meeting_id: The Vexa meeting ID to fetch transcripts for
            since_ts: Only return segments newer than this timestamp (epoch seconds).
                      If None, returns all available segments.
        
        Returns:
            List of normalized TranscriptUtterance objects
        
        Raises:
            VexaAPIError: On API errors
        """
        # TODO: Confirm actual Vexa API endpoint path
        endpoint = f"{self.base_url}/meetings/{meeting_id}/transcript"
        
        params = {}
        # TODO: Check if Vexa API supports server-side filtering by timestamp
        # If it does, uncomment and adjust:
        # if since_ts is not None:
        #     params["since"] = since_ts
        
        try:
            response = self._session.get(
                endpoint,
                params=params,
                timeout=self.timeout,
            )
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            logger.error(f"Vexa API request failed: {e}")
            raise VexaAPIError(f"Failed to fetch transcript: {e}") from e
        
        raw_data = response.json()
        utterances = self._normalize_response(raw_data, meeting_id)
        
        # Client-side filtering if API doesn't support since_ts
        if since_ts is not None:
            utterances = [u for u in utterances if u.start_ts > since_ts]
        
        logger.debug(f"Fetched {len(utterances)} utterances from meeting {meeting_id}")
        return utterances
    
    def _normalize_response(
        self,
        raw_data: dict,
        meeting_id: str,
    ) -> list[TranscriptUtterance]:
        """
        Normalize raw Vexa API response to canonical format.
        
        TODO: Adjust field mappings based on actual Vexa API response structure.
        
        Expected raw format (assumed):
        {
            "transcript": [
                {
                    "speaker_id": "...",
                    "speaker_name": "...",
                    "text": "...",
                    "start_time": 1234567890.123,
                    "end_time": 1234567891.456
                },
                ...
            ]
        }
        """
        utterances = []
        
        # TODO: Adjust based on actual Vexa API response structure
        transcript_items = raw_data.get("transcript", [])
        
        for item in transcript_items:
            try:
                utterance = TranscriptUtterance(
                    meeting_id=meeting_id,
                    speaker_id=str(item.get("speaker_id", "unknown")),
                    speaker_name=item.get("speaker_name"),
                    text=item.get("text", ""),
                    start_ts=float(item.get("start_time", 0)),
                    end_ts=item.get("end_time"),
                    source="vexa",
                )
                utterances.append(utterance)
            except (KeyError, ValueError, TypeError) as e:
                logger.warning(f"Failed to parse transcript item: {e}, item={item}")
                continue
        
        return utterances
    
    def check_meeting_active(self, meeting_id: str) -> bool:
        """
        Check if a meeting is currently active.
        
        TODO: Implement based on actual Vexa API capabilities.
        """
        # TODO: Implement actual meeting status check
        # For now, assume meeting is always active if we can reach the API
        try:
            endpoint = f"{self.base_url}/meetings/{meeting_id}/status"
            response = self._session.get(endpoint, timeout=self.timeout)
            response.raise_for_status()
            data = response.json()
            return data.get("status") == "active"
        except requests.exceptions.RequestException:
            # If we can't check, assume it's active and let fetch handle errors
            return True
    
    def close(self):
        """Close the HTTP session."""
        self._session.close()
        logger.info("VexaClient session closed")


class VexaAPIError(Exception):
    """Exception raised for Vexa API errors."""
    pass


# --- Mock client for testing without real API ---

class MockVexaClient(VexaClient):
    """
    Mock Vexa client for local testing.
    
    Generates fake transcript data to simulate the real API.
    Useful for testing the pipeline without Vexa credentials.
    """
    
    def __init__(self, meeting_id: str = "mock-meeting-001"):
        # Don't call super().__init__ since we don't need real API setup
        self.meeting_id = meeting_id
        self._utterance_counter = 0
        self._speakers = [
            ("speaker-1", "Alice"),
            ("speaker-2", "Bob"),
            ("speaker-3", "Charlie"),
        ]
        self._sample_texts = [
            "I think we should discuss the quarterly results.",
            "That's a great point. Let me add some context.",
            "Can we schedule a follow-up meeting for next week?",
            "I agree with the proposal, but we need more data.",
            "Let's move on to the next agenda item.",
            "Does anyone have questions about this topic?",
            "I'll send the report after this meeting.",
            "We should consider the budget implications.",
        ]
        logger.info("MockVexaClient initialized for testing")
    
    def fetch_latest_transcript(
        self,
        meeting_id: str,
        since_ts: Optional[float] = None,
    ) -> list[TranscriptUtterance]:
        """Generate fake transcript data for testing."""
        import random
        
        # Simulate 0-3 new utterances per poll
        num_new = random.randint(0, 3)
        utterances = []
        
        for _ in range(num_new):
            speaker_id, speaker_name = random.choice(self._speakers)
            text = random.choice(self._sample_texts)
            start_ts = time.time()
            
            utterance = TranscriptUtterance(
                meeting_id=meeting_id,
                speaker_id=speaker_id,
                speaker_name=speaker_name,
                text=text,
                start_ts=start_ts,
                end_ts=start_ts + random.uniform(1.0, 5.0),
                source="vexa",
            )
            utterances.append(utterance)
            self._utterance_counter += 1
        
        if utterances:
            logger.debug(f"MockVexaClient generated {len(utterances)} utterances")
        
        return utterances
    
    def check_meeting_active(self, meeting_id: str) -> bool:
        return True
    
    def close(self):
        logger.info("MockVexaClient closed")
