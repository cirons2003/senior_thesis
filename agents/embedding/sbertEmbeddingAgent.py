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


    def embed(self, texts: list[str]) -> list[list[float]]:      
        """
        Generate an embedding for the given batch of text.

        """
        return self.model.encode(texts, convert_to_tensor=True, show_progress_bar=False).cpu().tolist()