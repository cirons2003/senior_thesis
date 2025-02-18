from typing import Protocol
from .embeddings import Embeddings

## Chunking agents implement description chunking step 
class ChunkingAgent(Protocol):
    name: str

    ## Chunk a piece of text 
    def chunk(self, raw_text: str) -> list[str]:
        ...

## Embedding agents implement the chunk embeding step 
class EmbeddingAgent(Protocol):
    name: str

    ## Generate a text embedding for a piece of text 
    def embed(self, raw_text: list[str]) -> list[list[float]]:
        ...
    
## Clustering agents implement the topic learning step. 
class ClusteringAgent(Protocol):
    name: str
    topic_count: int
    
    ## Pass the filled Embeddings object used to access embeddings 
    def pass_embeddings(self, embeddings: Embeddings):
        ...

    ## Learn the topic centers. Pass_embeddings must have been called previously
    def train():
        ...

    ## Get result vector for a set of person embeddings
    def generate_result(self, person_embeddings: list[list[float]]) -> list[int]:
        ...

    ## True if train has been called previously 
    def is_finished_training() -> bool:
        ...
 