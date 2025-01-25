from typing import List
from ...helpers.embeddings import Embeddings

class MockClusteringAgent:
    def __init__(self, name: str = "MockClusteringAgent", topic_count: int = 3):
        self.name = name
        self.topic_count = topic_count
        self._is_trained = False
        self._embeddings = None

    def pass_embeddings(self, embeddings: Embeddings):
        """
        Simulates storing embeddings for training. 
        
        Args:
            embeddings (Embeddings): The embeddings object to be used for training.
        """
        self._embeddings = embeddings

    def train(self):
        """
        Simulates training by setting the `_is_trained` flag.
        Raises an error if embeddings have not been passed first.
        """
        if self._embeddings is None:
            raise Exception("Embeddings must be passed before training.")
        self._is_trained = True

    def generate_result(self, person_embeddings: List[List[int]]) -> List[int]:
        """
        Generates a mock result vector for a set of person embeddings.
        Each result is a simple deterministic calculation based on the sum of embeddings.
        
        Args:
            person_embeddings (List[int]): A list of embeddings for a person.

        Returns:
            List[int]: A mock binary vector indicating topic membership.
        """
        if not self._is_trained:
            raise Exception("Train must be called before generating results.")

        # Mock logic: Assign a binary topic vector based on the sum of embeddings
        result = [0 for _ in range(self.topic_count)]
        for i in range(len(person_embeddings)): 
            bucket = int(sum(person_embeddings[i]) * 100) % self.topic_count
            if 0 <= bucket < self.topic_count:
                result[bucket] = 1
            else:
                raise ValueError(f"Bucket value {bucket} is out of range for topic count {self.topic_count}.")


        return result
        
    def is_finished_training(self) -> bool:
        """
        Returns whether the training is finished.
        
        Returns:
            bool: True if the agent is trained, False otherwise.
        """
        return self._is_trained
