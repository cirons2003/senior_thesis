import sqlite3
import pytest
from senior_thesis.helpers.results import Results
import pickle


# Fixture to set up an in-memory database and initialize the Results class
@pytest.fixture
def setup_results():
    conn = sqlite3.connect(":memory:")  # In-memory database for testing
    cursor = conn.cursor()

    # Create the results table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS results_table (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        person_id INTEGER NOT NULL,
        result BLOB NOT NULL
    )
    """)
    conn.commit()

    # Initialize the Results class
    results = Results(conn, "results_table")
    return conn, results


# Test inserting results
def test_insert_result(setup_results):
    conn, results = setup_results

    # Insert a result
    results.insertResult(1, [1, 0, 1])

    # Verify the result is in the database
    cursor = conn.cursor()
    cursor.execute("SELECT person_id, result FROM results_table")
    row = cursor.fetchone()
    assert row[0] == 1
    assert pickle.loads(row[1]) == [1, 0, 1]


# Test retrieving all results
def test_get_all_results(setup_results):
    conn, results = setup_results

    # Insert multiple results
    results.insertResult(1, [1, 0, 1])
    results.insertResult(2, [0, 1, 0])
    results.insertResult(3, [1, 1, 1])

    # Retrieve all results
    all_results = results.getAllResults()
    assert all_results == [[1, 0, 1], [0, 1, 0], [1, 1, 1]]


# Test fetching results in batches
def test_get_person_results_batch(setup_results):
    conn, results = setup_results

    # Insert multiple results
    results.insertResult(1, [1, 0, 1])
    results.insertResult(2, [0, 1, 0])
    results.insertResult(3, [1, 1, 1])

    # Fetch the first batch
    batch = results.getPersonResultsBatch(start_id=1, batch_size=2)
    assert batch == [(1, [1, 0, 1]), (2, [0, 1, 0])]

    # Fetch the remaining results
    batch = results.getPersonResultsBatch(start_id=3, batch_size=2)
    assert batch == [(3, [1, 1, 1])]

    # Fetch batch starting from a non-existent ID
    batch = results.getPersonResultsBatch(start_id=4, batch_size=2)
    assert batch == []


# Test behavior when the table is empty
def test_empty_table_behavior(setup_results):
    conn, results = setup_results

    # Fetch all results from an empty table
    all_results = results.getAllResults()
    assert all_results == []

    # Fetch a batch from an empty table
    batch = results.getPersonResultsBatch(start_id=1, batch_size=2)
    assert batch == []
