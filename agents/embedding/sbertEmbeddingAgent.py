from sentence_transformers import SentenceTransformer
from typing import List

class SBERTEmbeddingAgent:
    def __init__(self, model_name: str = "all-MiniLM-L6-v2", device: str = 'cuda'):
        """
        Initialize the SBERT embedding agent with a specified model.

        Args:
            model_name (str): The name of the SBERT model to use. 
                              Defaults to 'all-MiniLM-L6-v2'.
        """
        self.name = f"SBERT"
        self.model_name = model_name
        self.device = device
        self.model = SentenceTransformer(model_name, device = device)

    def embed(self, text: str) -> List[float]:
        """
        Generate an embedding for the given text.

        Args:
            text (str): The input text to embed.

        Returns:
            List[float]: The embedding vector.
        """
        return self.model.encode(text).tolist()

    def chunk_and_embed(self, text: str, chunker) -> List[List[float]]:
        """
        Chunk the input text using a chunker and generate embeddings for each chunk.

        Args:
            text (str): The input text to process.
            chunker: A chunking agent implementing the `chunk` method.

        Returns:
            List[List[float]]: A list of embedding vectors for each chunk.
        """
        chunks = chunker.chunk(text)
        return [self.embed(chunk) for chunk in chunks]
