# Whisper-Assistant

A proactive assistant enabling real-time, context-aware AI interventions at key human decision moments.

## Phase 1: Ingestion Pipeline

Real-time meeting transcript ingestion from Vexa to Qdrant vector database.

### Prerequisites

- Python 3.10+
- Docker (for Qdrant)
- Vexa API credentials

### Quick Start

1. **Start Qdrant:**
   ```bash
   docker run -p 6333:6333 qdrant/qdrant
   ```

2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure environment:**
   ```bash
   cp .env.example .env
   # Edit .env with your Vexa credentials
   ```

4. **Run the pipeline:**
   ```bash
   # With real Vexa API:
   export VEXA_API_KEY="your-key"
   export VEXA_MEETING_ID="meeting-id"
   python -m ingestion.main

   # For testing with mock data:
   python -m ingestion.main --mock
   ```

### Architecture

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────┐
│   Vexa API      │────▶│ Ingestion Loop   │────▶│   Qdrant    │
│ (Transcripts)   │     │ (Poll & Dedupe)  │     │  (Vectors)  │
└─────────────────┘     └──────────────────┘     └─────────────┘
```

### Components

- **VexaClient**: Fetches live transcripts from Vexa Meeting Bot API
- **TranscriptUtterance**: Canonical data model for utterances
- **QdrantStore**: Vector storage with deduplication
- **IngestionPipeline**: Coordinates polling and storage

### Configuration

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `VEXA_API_KEY` | Yes | - | Vexa API authentication key |
| `VEXA_MEETING_ID` | Yes | - | Meeting ID to monitor |
| `VEXA_API_URL` | No | `https://api.vexa.ai/v1` | Custom API URL |
| `QDRANT_HOST` | No | `localhost` | Qdrant server host |
| `QDRANT_PORT` | No | `6333` | Qdrant server port |
| `POLL_INTERVAL` | No | `2.0` | Seconds between polls |

### Future Hooks

The pipeline is designed for extension:

- **Real embeddings**: Replace `_placeholder_embed()` in `QdrantStore`
- **Decision-moment detector**: Use `on_new_utterance` callback in `IngestionPipeline`
- **Streaming**: Extend to WebSocket/SSE when Vexa supports it
