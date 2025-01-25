from ..metadata import MetaData
from ..setup import initialize_database_tables
import sqlite3
import pytest

iter_count = 3

@pytest.fixture
def setup_metadata():
    conn = sqlite3.connect(":memory:")  # In-memory database for testing

    # Create metadata table
    initialize_database_tables(conn)

    yield conn

    conn.close()

def test_metadata(setup_metadata): 
    conn = setup_metadata
    m = MetaData(conn, 'test_trial')

    assert(m.get_current_epoch() == 0) 
    assert(m.get_current_index() == 0)
    assert(m.get_current_stage() == 0)
    assert(m.get_person_count() == 0)
    assert(m.get_topic_count() == 0)

    # test incrementing assertions 
    for val in [0, 2, 256]:
        with pytest.raises(AssertionError, match="current_stage must increment by 1"):
            m.update_current_stage(val)
        with pytest.raises(AssertionError, match="current_epoch must increment by 1"):
            m.update_current_epoch(val)
        

    # test non-negative assertions 
    for val in [-1, -200, -99999]: 
        with pytest.raises(AssertionError):
            m.update_current_epoch(val)
        with pytest.raises(AssertionError):
            m.update_current_stage(val)
        with pytest.raises(AssertionError):
            m.update_current_index(val)
        with pytest.raises(AssertionError):
            m.update_person_count(val)
        with pytest.raises(AssertionError): 
            m.update_topic_count(val)

    with pytest.raises(AssertionError): 
        m.update_topic_count(0)

    # test current_epoch updates 
    for i in range(iter_count): 
        m.update_current_epoch(i + 1)
        assert(m.get_current_epoch() == i + 1)

    # test current_stage updates
    for i in range(iter_count): 
        m.update_current_stage(i + 1)
        assert(m.get_current_stage() == i + 1)
    
    # test current_index updates
    for val in [1, 2, 0, 201, 1526, 99999]: 
        m.update_current_index(val)
        assert(m.get_current_index() == val)

    # test current_index updates
    for val in [1, 2, 0, 201, 1526, 99999]: 
        m.update_person_count(val)
        assert(m.get_person_count() == val)

    # ensure topic_count can only be updated once 
    m.update_topic_count(12) 
    assert(m.get_topic_count() == 12)
    for val in [1, 9, 18, 200]:
        with pytest.raises(AssertionError):
            m.update_topic_count(val)
    m.update_topic_count(12)
    assert(m.get_topic_count() == 12)

 
def test_update_and_get_table_names(setup_metadata):
    conn = setup_metadata

    # Initialize metadata for a trial
    trial_name = "test_trial"
    metadata = MetaData(conn, trial_name)

    # Update table names
    metadata.update_embeddings_table_name("embeddings_table_test")
    metadata.update_results_table_name("results_table_test")
    metadata.update_index_table_name("index_table_test")

    # Retrieve and assert table names
    assert metadata.get_embeddings_table_name() == "embeddings_table_test"
    assert metadata.get_results_table_name() == "results_table_test"
    assert metadata.get_index_table_name() == "index_table_test"

