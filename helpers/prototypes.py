from typing import Protocol

class ChunkingAgent(Protocol):
    name: str

    def chunk(self, raw_text: str) -> list[str]:
        ...

class EmbeddingAgent(Protocol):
    name: str

    def embed(self, raw_text: str) -> list[int]:
        ...
    
class ClusteringAgent(Protocol):
    name: str

    def train(self, batch: list[list[int]]) -> None:
        ...

    def get_centers(self) -> list[list[int]]:
        ...

    def lock(self) -> None:
        ...

    def unlock(self) -> None:
        ...

    def classify(self, embedding: list[int]) -> int:
        ...


