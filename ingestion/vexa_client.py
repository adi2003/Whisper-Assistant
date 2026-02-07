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
    
    DEFAULT_BASE_URL = "https://api.cloud.vexa.ai"
    
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
        self._bot_id: Optional[str] = None
        self._session = requests.Session()
        self._session.headers.update({
            "X-API-Key": api_key,
            "Content-Type": "application/json",
        })
        logger.info(f"VexaClient initialized, base_url={self.base_url}")
    
    def deploy_bot(
        self,
        native_meeting_id: str,
        platform: str = "google_meet",
        bot_name: str = "WhisperAssistant",
    ) -> str:
        """
        Deploy a bot to join a meeting.
        
        Args:
            native_meeting_id: The platform-specific meeting ID (e.g., "abc-defg-hij")
            platform: Meeting platform ("google_meet", "zoom", "teams", etc.)
            bot_name: Display name for the bot in the meeting
        
        Returns:
            The bot ID for subsequent transcript fetches
        
        Raises:
            VexaAPIError: On API errors
        """
        endpoint = f"{self.base_url}/bots"
        payload = {
            "platform": platform,
            "native_meeting_id": native_meeting_id,
            "bot_name": bot_name,
        }
        
        try:
            response = self._session.post(
                endpoint,
                json=payload,
                timeout=self.timeout,
            )
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to deploy bot: {e}")
            raise VexaAPIError(f"Failed to deploy bot: {e}") from e
        
        data = response.json()
        # TODO: Confirm response structure - assuming {"bot_id": "..."}
        self._bot_id = data.get("bot_id") or data.get("id")
        logger.info(f"Bot deployed successfully, bot_id={self._bot_id}")
        return self._bot_id
    
    @property
    def bot_id(self) -> Optional[str]:
        """Get the current bot ID."""
        return self._bot_id
    
    @bot_id.setter
    def bot_id(self, value: str):
        """Set the bot ID (for resuming a session)."""
        self._bot_id = value
    
    def fetch_latest_transcript(
        self,
        meeting_id: str,
        since_ts: Optional[float] = None,
    ) -> list[TranscriptUtterance]:
        """
        Fetch transcript segments from a meeting.
        
        Args:
            meeting_id: The bot ID or meeting ID to fetch transcripts for.
                        If bot was deployed via deploy_bot(), uses the stored bot_id.
            since_ts: Only return segments newer than this timestamp (epoch seconds).
                      If None, returns all available segments.
        
        Returns:
            List of normalized TranscriptUtterance objects
        
        Raises:
            VexaAPIError: On API errors
        """
        # Use stored bot_id if available, otherwise use provided meeting_id
        bot_id = self._bot_id or meeting_id
        # TODO: Confirm actual Vexa API endpoint path for transcripts
        endpoint = f"{self.base_url}/bots/{bot_id}/transcript"
        
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
        print(f"Raw Vexa API response: {raw_data}")  # Debug log to inspect response structure
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
    
    def get_bot_status(self, bot_id: Optional[str] = None) -> dict:
        """
        Get the status of a deployed bot.
        
        Args:
            bot_id: The bot ID to check. If None, uses stored bot_id.
        
        Returns:
            Bot status information dict
        """
        bot_id = bot_id or self._bot_id
        if not bot_id:
            raise VexaAPIError("No bot_id available. Deploy a bot first.")
        
        try:
            endpoint = f"{self.base_url}/bots/{bot_id}"
            response = self._session.get(endpoint, timeout=self.timeout)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to get bot status: {e}")
            raise VexaAPIError(f"Failed to get bot status: {e}") from e
    
    def check_meeting_active(self, meeting_id: str) -> bool:
        """
        Check if a meeting/bot session is currently active.
        """
        try:
            status = self.get_bot_status(meeting_id)
            # TODO: Confirm status field name from API response
            return status.get("status") in ("active", "joined", "recording")
        except VexaAPIError:
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
