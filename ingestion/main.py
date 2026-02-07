#!/usr/bin/env python3
"""
Main entrypoint for the Whisper-Assistant ingestion pipeline.

Runs a long-running process that:
1. Connects to Vexa API to fetch live meeting transcripts
2. Stores utterances in Qdrant for vector search

Usage:
    # With real Vexa API:
    export VEXA_API_KEY="your-api-key"
    export VEXA_MEETING_ID="meeting-id"
    python -m ingestion.main

    # With mock client for testing:
    python -m ingestion.main --mock

Environment Variables:
    VEXA_API_KEY: API key for Vexa authentication
    VEXA_MEETING_ID: Native meeting ID (e.g., "abc-defg-hij" for Google Meet)
    VEXA_PLATFORM: Meeting platform (google_meet, zoom, teams)
    VEXA_BOT_NAME: Bot display name in meeting
    VEXA_API_URL: (optional) Custom Vexa API base URL
    QDRANT_HOST: (optional) Qdrant host, default: localhost
    QDRANT_PORT: (optional) Qdrant port, default: 6333
    POLL_INTERVAL: (optional) Seconds between polls, default: 2.0
"""

import argparse
import logging
import os
import sys

from .vexa_client import VexaClient, MockVexaClient, VexaAPIError
from .qdrant_store import QdrantStore
from .ingestion import IngestionPipeline

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def get_config() -> dict:
    """Load configuration from environment variables."""
    return {
        "vexa_api_key": os.environ.get("VEXA_API_KEY"),
        "vexa_meeting_id": os.environ.get("VEXA_MEETING_ID"),
        "vexa_api_url": os.environ.get("VEXA_API_URL"),
        "vexa_platform": os.environ.get("VEXA_PLATFORM", "google_meet"),
        "vexa_bot_name": os.environ.get("VEXA_BOT_NAME", "WhisperAssistant"),
        "qdrant_host": os.environ.get("QDRANT_HOST", "localhost"),
        "qdrant_port": int(os.environ.get("QDRANT_PORT", "6333")),
        "poll_interval": float(os.environ.get("POLL_INTERVAL", "2.0")),
    }


def validate_config(config: dict, use_mock: bool) -> bool:
    """Validate required configuration."""
    if not use_mock:
        if not config["vexa_api_key"]:
            logger.error("VEXA_API_KEY environment variable is required")
            return False
        if not config["vexa_meeting_id"]:
            logger.error("VEXA_MEETING_ID environment variable is required")
            return False
    return True


def main():
    """Main entrypoint."""
    parser = argparse.ArgumentParser(
        description="Whisper-Assistant Transcript Ingestion Pipeline"
    )
    parser.add_argument(
        "--mock",
        action="store_true",
        help="Use mock Vexa client for testing (no real API calls)",
    )
    parser.add_argument(
        "--max-iterations",
        type=int,
        default=None,
        help="Maximum number of poll iterations (for testing)",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable debug logging",
    )
    parser.add_argument(
        "--skip-deploy",
        action="store_true",
        help="Skip bot deployment (use existing bot_id from VEXA_MEETING_ID)",
    )
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Load config
    config = get_config()
    
    if not validate_config(config, args.mock):
        sys.exit(1)
    
    # Initialize clients
    if args.mock:
        logger.info("Using mock Vexa client for testing")
        meeting_id = config["vexa_meeting_id"] or "mock-meeting-001"
        vexa_client = MockVexaClient(meeting_id=meeting_id)
        bot_id = meeting_id
    else:
        meeting_id = config["vexa_meeting_id"]
        vexa_client = VexaClient(
            api_key=config["vexa_api_key"],
            base_url=config["vexa_api_url"],
        )
        
        # Deploy bot to join the meeting
        if args.skip_deploy:
            # Use meeting_id as bot_id directly (for resuming sessions)
            vexa_client.bot_id = meeting_id
            bot_id = meeting_id
            logger.info(f"Skipping bot deployment, using bot_id={bot_id}")
        else:
            try:
                bot_id = vexa_client.deploy_bot(
                    native_meeting_id=meeting_id,
                    platform=config["vexa_platform"],
                    bot_name=config["vexa_bot_name"],
                )
                logger.info(f"Bot deployed to meeting, bot_id={bot_id}")
            except VexaAPIError as e:
                logger.error(f"Failed to deploy bot: {e}")
                sys.exit(1)
    
    # Connect to Qdrant
    try:
        qdrant_store = QdrantStore(
            host=config["qdrant_host"],
            port=config["qdrant_port"],
        )
        logger.info(
            f"Connected to Qdrant at {config['qdrant_host']}:{config['qdrant_port']}"
        )
    except Exception as e:
        logger.error(f"Failed to connect to Qdrant: {e}")
        logger.error(
            "Ensure Qdrant is running: docker run -p 6333:6333 qdrant/qdrant"
        )
        sys.exit(1)
    
    # Create and run pipeline
    pipeline = IngestionPipeline(
        vexa_client=vexa_client,
        qdrant_store=qdrant_store,
        meeting_id=bot_id,  # Use bot_id for transcript fetching
        poll_interval=config["poll_interval"],
        # TODO: Add on_new_utterance callback for downstream processing
        # on_new_utterance=decision_moment_detector.process,
    )
    
    logger.info("=" * 60)
    logger.info("Whisper-Assistant Ingestion Pipeline")
    logger.info(f"Meeting ID: {meeting_id}")
    logger.info(f"Bot ID: {bot_id}")
    logger.info(f"Platform: {config['vexa_platform']}")
    logger.info(f"Poll Interval: {config['poll_interval']}s")
    logger.info(f"Mode: {'Mock' if args.mock else 'Live'}")
    logger.info("=" * 60)
    logger.info("Press Ctrl+C to stop")
    
    try:
        pipeline.run(max_iterations=args.max_iterations)
    finally:
        vexa_client.close()
        qdrant_store.close()
        logger.info("Shutdown complete")


if __name__ == "__main__":
    main()
