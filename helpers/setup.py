import sqlite3

##Throws errors
def initialize_database_tables (conn: sqlite3.Connection):
    if conn.in_transaction:
        conn.rollback()

    tables = [
        """
        CREATE TABLE IF NOT EXISTS persons (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            description TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS metadata (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trial_name TEXT UNIQUE NOT NULL,
            embeddings_table_name TEXT NOT NULL DEFAULT "", 
            results_table_name TEXT NOT NULL DEFAULT "", 
            index_table_name TEXT NOT NULL DEFAULT "",
            current_stage INTEGER DEFAULT 0,
            current_index INTEGER DEFAULT 0,
            current_epoch INTEGER DEFAULT 0,
            person_count INTEGER DEFAULT 0, 
            topic_count INTEGER DEFAULT 0
        )
        """
    ]

    
    cursor = conn.cursor()
    for table in tables:
        cursor.execute(table)
    conn.commit()

"""
-- Problem:
-- The dataset consists of embeddings for multiple individuals, 
-- with each individual having a variable number of embeddings stored sequentially. 
-- Sequential storage introduces bias in clustering and sampling processes, 
-- as individuals with more embeddings disproportionately influence results.

-- Solution:
-- To ensure fair and unbiased representation, each individual's embeddings are 
-- indexed (`embedding_id`) and paired with the total count of embeddings they have (`total_embeddings`). 
-- During sampling, embeddings are selected using a modulo operation with a random value 
-- and the total embedding count, ensuring each person has an equal influence on the clusters.

-- Design Choices:
-- 1. Added `embedding_id`: Sequential index for each embedding within a person's data.
-- 2. Added `total_embeddings`: Represents the total number of embeddings per person, 
--    allowing dynamic filtering based on modulo logic.
-- 3. Random Sampling: A random value modulated by `total_embeddings` ensures that 
--    every embedding has an equal chance of being included in a batch, 
--    regardless of the total number of embeddings for a person.
-- 4. Efficient Querying: The design avoids computationally expensive sorting operations 
--    (e.g., `ORDER BY RANDOM()`) by precomputing indices and embedding counts, 
--    allowing efficient random batch selection.

-- This design ensures unbiased representation of individuals in clustering and is 
-- scalable for large datasets.
"""

def initialize_embeddings_table(conn: sqlite3.Connection, table_name: str)-> None:
    cursor = conn.cursor()
    
    query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id INTEGER PRIMARY KEY AUTOINCREMENT, 
        person_id INTEGER NOT NULL,                   
        embedding_id INTEGER NOT NULL,
        total_embeddings INTEGER NOT NULL,
        embedding BLOB NOT NULL,      
        FOREIGN KEY (person_id) REFERENCES persons (id)
            ON DELETE CASCADE                    
    )
    """

    cursor.execute(query)
    cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_embedding_composite ON {table_name}(embedding_id, total_embeddings);") #optimization for sampling
    conn.commit()

def initialize_centers_table(conn: sqlite3.Connection, table_name: str) -> None:
    cursor = conn.cursor()

    query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id INTEGER PRIMARY KEY AUTOINCREMENT, 
        center BLOB NOT NULL                  
    )
    """

    cursor.execute(query)
    conn.commit()

def initialize_results_table(conn: sqlite3.Connection, table_name: str) -> None:
    cursor = conn.cursor()
    
    query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id INTEGER PRIMARY KEY AUTOINCREMENT, 
        person_id INTEGER NOT NULL,              
        result BLOB NOT NULL,                    
        FOREIGN KEY (person_id) REFERENCES persons (id)
            ON DELETE CASCADE                    
    )
    """

    cursor.execute(query)
    conn.commit()


def initialize_index_table(conn: sqlite3.Connection, table_name: str) -> None: 
    cursor = conn.cursor()

    query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id INTEGER PRIMARY KEY AUTOINCREMENT, 
        topic_id INTEGER NOT NULL,
        person_id INTEGER NOT NULL,
        feature_count INTEGER NOT NULL,
        FOREIGN KEY (person_id) REFERENCES persons (id)
            ON DELETE CASCADE  
    )
    """ 
    cursor.execute(query)
    cursor.execute(f'CREATE INDEX IF NOT EXISTS idx_topic_person ON {table_name}(topic_id, person_id)')
    conn.commit()