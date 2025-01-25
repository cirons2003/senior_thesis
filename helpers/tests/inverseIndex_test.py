import pytest
import sqlite3
from ..inverseIndex import InverseIndex
from ..setup import initialize_index_table

# Fixture to set up an in-memory database and the InverseIndex class
@pytest.fixture
def setup_database():
    conn = sqlite3.connect(":memory:")  # In-memory database for testing
    initialize_index_table(conn, "inverse_index")
    return conn, InverseIndex(conn, "inverse_index")

def test_add_result_vector(setup_database):
    conn, index = setup_database

    # Add a result vector
    index.add_result_vector([1, 2, 3], person_id=42, feature_count=5)

    cursor = conn.cursor()
    cursor.execute("SELECT topic_id, person_id, feature_count FROM inverse_index")
    results = cursor.fetchall()

    assert len(results) == 3
    assert (1, 42, 5) in results
    assert (2, 42, 5) in results
    assert (3, 42, 5) in results

def test_get_all_documents_by_topics_distinct(setup_database):
    conn, index = setup_database

    # Add result vectors
    index.add_result_vector([1, 2], person_id=42, feature_count=5)
    index.add_result_vector([2, 3], person_id=43, feature_count=3)

    # Retrieve documents for topic_ids = [2, 3]
    result = index.get_all_documents_by_topics([2, 3])

    assert len(result) == 2
    assert 42 in result
    assert 43 in result

def test_get_all_documents_by_topics_non_distinct(setup_database):
    conn, index = setup_database

    # Add result vectors
    index.add_result_vector([1, 2], person_id=42, feature_count=5)
    index.add_result_vector([2, 3], person_id=43, feature_count=3)

    # Retrieve documents for topic_ids = [2, 3]
    result = index.get_all_documents_by_topics([2, 3], distinct = False)

    assert len(result) == 3
    assert 42 in result
    assert 43 in result
