import re
from ..embeddings import Embeddings

class MockChunkingAgent(): 
    def __init__(self, name: str): 
        self.name: str = name
        return 
    
    def chunk(self, raw_text: str) -> list[str]:
        return re.split(r'(?<=[.!?]) +', raw_text)
    
class MockEmbeddingAgent:
    def __init__(self, name: str):
        self.name: str = name

    def embed(self, raw_text: str) -> list[int]:
        return [len(raw_text)] if raw_text else [0]  

class MockClusteringAgent:
    def __init__(self, name: str, topic_count: int = 1):
        self.name: str = name
        self.topic_count: int = topic_count
        self._locked = True
        self._is_finished_training = False

    def pass_embeddings(self, embeddings: Embeddings):
        self._locked = False  
    
    def train(self):
        if self._locked:
            raise Exception("Locked! Call `pass_embeddings` first.")
        self._is_finished_training = True

    def generate_result(self, person_embeddings: list[int]) -> list[int]:
        if not self._is_finished_training:
            raise Exception("Finish training first.")
        return [e[0] for e in person_embeddings]  

    def is_finished_training(self) -> bool:
        return self._is_finished_training
