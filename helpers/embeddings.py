import sqlite3 
import pickle

"""
CREATE TABLE IF NOT EXISTS embeddings (
    id INTEGER PRIMARY KEY AUTOINCREMENT, 
    person_id INTEGER NOT NULL,              
    embedding BLOB NOT NULL,                 
    FOREIGN KEY (person_id) REFERENCES persons (id)
        ON DELETE CASCADE                    
)
"""

# Design Decision: The client controls connection, commit, and close operations
# to allow batching of inserts, reducing latency from multiple commits.
# This approach optimizes performance for large-scale batch processing.


##Throws Errors
class Embeddings(): 
    def __init__(self, conn: sqlite3.Connection, table_name: str) -> None: 
        self.table_name = table_name
        self.conn = conn

    def insertEmbedding(self, person_index: int, embedding: list[int]) -> None: 
            cursor = self.conn.cursor()

            embedding_blob = pickle.dumps(embedding)

            cursor.execute(f"INSERT INTO persons (person_id, embedding) VALUES ({person_index}, {embedding_blob})")

    