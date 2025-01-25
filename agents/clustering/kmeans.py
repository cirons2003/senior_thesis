from sklearn.cluster import KMeans
from typing import List
from ...helpers.embeddings import Embeddings

class KMeansClusteringAgent:
    def __init__(self, topic_count: int):
        """
        Initializes the KMeansClusteringAgent.

        Args:
            name (str): The name of the agent.
            topic_count (int): The number of clusters (topics) to learn.
        """
        self.name = f'KMEANSx{topic_count}'
        self.topic_count = topic_count
        self._kmeans = KMeans(n_clusters=topic_count, random_state=42)
        self._is_trained = False
        self._embeddings = None

    def pass_embeddings(self, embeddings: Embeddings):
        """
        Passes the filled Embeddings object to the clustering agent.

        Args:
            embeddings (Embeddings): The embeddings object used for clustering.
        """
        self._embeddings = embeddings

    def train(self):
        """
        Trains the KMeans model using the passed embeddings.
        """
        if self._embeddings is None:
            raise Exception("Embeddings must be passed before training.")

        # Retrieve all embeddings from the Embeddings object
        all_embeddings = self._embeddings.getAllEmbeddings()

        if not all_embeddings:
            raise Exception("No embeddings found for training.")

        # Fit the KMeans model
        self._kmeans.fit(all_embeddings)
        self._is_trained = True

    def generate_result(self, person_embeddings: List[List[int]]) -> List[int]:
        """
        Generates a binary topic membership vector for a set of person embeddings.

        Args:
            person_embeddings (List[List[int]]): A list of embeddings for a person.

        Returns:
            List[int]: A binary vector indicating the topics (clusters) the person belongs to.
        """
        if not self._is_trained:
            raise Exception("Train must be called before generating results.")

        # Predict the clusters for each embedding
        cluster_assignments = self._kmeans.predict(person_embeddings)

        # Generate a binary vector indicating topic membership
        result_vector = [0] * self.topic_count
        for cluster in cluster_assignments:
            result_vector[cluster] = 1

        return result_vector

    def is_finished_training(self) -> bool:
        """
        Returns whether the training is complete.

        Returns:
            bool: True if the model is trained, False otherwise.
        """
        return self._is_trained
