import pytest
from ..researchRunner import ResearchRunner
from ..persons import Persons
from ..metadata import MetaData
from ..embeddings import Embeddings
from ..centers import Centers
from ..results import Results
from .testAgents import MockChunkingAgent, MockEmbeddingAgent, MockClusteringAgent
from ..setup import initialize_database_tables
import sqlite3

@pytest.fixture
def setup_database():
    """Fixture to set up an in-memory database and test data."""
    database_path = ":memory:"
    trial_name = "test_trial"

    # Initialize test agents
    chunking_agent = MockChunkingAgent(name="test_chunking")
    embedding_agent = MockEmbeddingAgent(name="test_embedding")
    clustering_agent = MockClusteringAgent(name="test_clustering", topic_count=3)

    conn = sqlite3.connect(database_path)
    initialize_database_tables(conn)

    # Initialize ResearchRunner
    runner = ResearchRunner(
        conn,
        trial_name,
        chunking_agent,
        embedding_agent,
        clustering_agent
    )

    # Add sample data to Persons table
    runner.persons.insertDescription(0, "This is the first test description.")
    runner.persons.insertDescription(1, "Another example description for testing.")
    runner.persons.insertDescription(2, "Final description for this integration test.")

    yield runner  # Pass the initialized runner to the test

    # Cleanup
    runner.conn.close()

def test_research_runner_pipeline(setup_database):
    """Integration test for ResearchRunner with test agents."""
    runner: ResearchRunner = setup_database

    # Run the research pipeline
    runner.run_research()

    # Verify the stages progressed correctly
    assert runner.metadata.get_current_stage() == 3  # Stage 3 == DONE
    assert runner.metadata.get_person_count() == 3  # 3 persons in total

    # Verify embeddings were generated
    embeddings = runner.embeddings.getAllEmbeddings()
    assert len(embeddings) > 0  # Embeddings should exist
    assert all(len(e) > 0 for e in embeddings)  # Embedding vectors should not be empty

    # Verify clusters were trained
    assert runner.clustering_agent.is_finished_training()

    # Verify results were computed
    results = runner.results.getAllResults()
    assert len(results) == 3  # One result per person
    for result_vector in results:
        assert len(result_vector) > 0  # Result vector should not be empty

    print("Integration test passed!")
