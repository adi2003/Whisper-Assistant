"""
Qdrant vector store for transcript storage.

Handles collection creation, point upserts, and embedding storage.
"""

import logging
from typing import Callable, Optional

from qdrant_client import QdrantClient
from qdrant_client.http import models as qdrant_models
from qdrant_client.http.exceptions import UnexpectedResponse

from .models import TranscriptUtterance

logger = logging.getLogger(__name__)

# Constants
COLLECTION_NAME = "meeting_transcripts"
VECTOR_SIZE = 384  # Placeholder size, common for sentence-transformers
DISTANCE = qdrant_models.Distance.COSINE


class QdrantStore:
    """
    Vector store for meeting transcripts using Qdrant.
    
    Stores transcript utterances with embeddings for semantic search.
    Currently uses placeholder embeddings; real embeddings can be
    plugged in via the embed_fn parameter.
    """
    
    def __init__(
        self,
        host: str = "localhost",
        port: int = 6333,
        collection_name: str = COLLECTION_NAME,
        vector_size: int = VECTOR_SIZE,
        embed_fn: Optional[Callable[[str], list[float]]] = None,
    ):
        """
        Initialize Qdrant store.
        
        Args:
            host: Qdrant server host
            port: Qdrant server port
            collection_name: Name of the collection to use
            vector_size: Dimension of embedding vectors
            embed_fn: Optional function to generate embeddings from text.
                      If None, uses placeholder zero vectors.
        """
        self.host = host
        self.port = port
        self.collection_name = collection_name
        self.vector_size = vector_size
        self._embed_fn = embed_fn or self._placeholder_embed
        
        self._client = QdrantClient(host=host, port=port)
        self._ensure_collection_exists()
        
        logger.info(
            f"QdrantStore initialized: {host}:{port}, "
            f"collection={collection_name}, vector_size={vector_size}"
        )
    
    def _placeholder_embed(self, text: str) -> list[float]:
        """
        Placeholder embedding function.
        
        Returns a zero vector. Replace with real embeddings in production.
        
        TODO: Replace with actual embedding model (e.g., sentence-transformers)
        """
        return [0.0] * self.vector_size
    
    def _ensure_collection_exists(self):
        """Create the collection if it doesn't exist."""
        try:
            collections = self._client.get_collections().collections
            exists = any(c.name == self.collection_name for c in collections)
            
            if not exists:
                self._client.create_collection(
                    collection_name=self.collection_name,
                    vectors_config=qdrant_models.VectorParams(
                        size=self.vector_size,
                        distance=DISTANCE,
                    ),
                )
                logger.info(f"Created Qdrant collection: {self.collection_name}")
            else:
                logger.info(f"Using existing Qdrant collection: {self.collection_name}")
                
        except Exception as e:
            logger.error(f"Failed to ensure collection exists: {e}")
            raise QdrantStoreError(f"Collection setup failed: {e}") from e
    
    def upsert_utterance(self, utterance: TranscriptUtterance) -> bool:
        """
        Insert or update a single utterance in the store.
        
        Args:
            utterance: The transcript utterance to store
            
        Returns:
            True if successful, False otherwise
        """
        return self.upsert_utterances([utterance])
    
    def upsert_utterances(self, utterances: list[TranscriptUtterance]) -> bool:
        """
        Insert or update multiple utterances in the store.
        
        Args:
            utterances: List of transcript utterances to store
            
        Returns:
            True if successful, False otherwise
        """
        if not utterances:
            return True
        
        points = []
        for utterance in utterances:
            point_id = utterance.compute_id()
            embedding = self._embed_fn(utterance.text)
            payload = utterance.to_dict()
            
            points.append(
                qdrant_models.PointStruct(
                    id=point_id,
                    vector=embedding,
                    payload=payload,
                )
            )
        
        try:
            self._client.upsert(
                collection_name=self.collection_name,
                points=points,
            )
            logger.info(f"Upserted {len(points)} utterances to Qdrant")
            return True
            
        except UnexpectedResponse as e:
            logger.error(f"Qdrant upsert failed: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error during upsert: {e}")
            return False
    
    def get_utterance_count(self) -> int:
        """Get the total number of utterances in the collection."""
        try:
            info = self._client.get_collection(self.collection_name)
            return info.points_count
        except Exception as e:
            logger.error(f"Failed to get utterance count: {e}")
            return -1
    
    def utterance_exists(self, utterance: TranscriptUtterance) -> bool:
        """Check if an utterance already exists in the store."""
        point_id = utterance.compute_id()
        try:
            result = self._client.retrieve(
                collection_name=self.collection_name,
                ids=[point_id],
            )
            return len(result) > 0
        except Exception:
            return False
    
    def search_similar(
        self,
        text: str,
        limit: int = 10,
    ) -> list[dict]:
        """
        Search for similar utterances by text.
        
        TODO: This is a placeholder for future semantic search.
        Currently returns empty since we use zero vectors.
        
        Args:
            text: Query text
            limit: Maximum number of results
            
        Returns:
            List of matching utterances with scores
        """
        embedding = self._embed_fn(text)
        
        try:
            results = self._client.search(
                collection_name=self.collection_name,
                query_vector=embedding,
                limit=limit,
            )
            return [
                {
                    "score": hit.score,
                    "utterance": TranscriptUtterance.from_dict(hit.payload),
                }
                for hit in results
            ]
        except Exception as e:
            logger.error(f"Search failed: {e}")
            return []
    
    def delete_meeting(self, meeting_id: str) -> bool:
        """
        Delete all utterances for a specific meeting.
        
        Args:
            meeting_id: The meeting ID to delete
            
        Returns:
            True if successful
        """
        try:
            self._client.delete(
                collection_name=self.collection_name,
                points_selector=qdrant_models.FilterSelector(
                    filter=qdrant_models.Filter(
                        must=[
                            qdrant_models.FieldCondition(
                                key="meeting_id",
                                match=qdrant_models.MatchValue(value=meeting_id),
                            )
                        ]
                    )
                ),
            )
            logger.info(f"Deleted utterances for meeting: {meeting_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to delete meeting {meeting_id}: {e}")
            return False
    
    def close(self):
        """Close the Qdrant client connection."""
        self._client.close()
        logger.info("QdrantStore connection closed")


class QdrantStoreError(Exception):
    """Exception raised for Qdrant store errors."""
    pass
