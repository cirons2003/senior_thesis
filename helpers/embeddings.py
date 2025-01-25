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

    def insertEmbedding(self, person_index: int, embedding: list[int], embedding_id: int, total_embeddings: int) -> None: 
            cursor = self.conn.cursor()

            embedding_blob = pickle.dumps(embedding)

            cursor.execute(f"""INSERT INTO {self.table_name} (person_id, embedding_id, total_embeddings, embedding) 
                           VALUES (?, ?, ?, ?)""", (person_index, embedding_id, total_embeddings, embedding_blob,))
    
    def getEmbeddingBatch(self, start_row: int, batch_size: int, epoch: int) -> tuple[int, list[list[int]]]:
        cursor = self.conn.cursor()

        cursor.execute(f"""SELECT id, embedding FROM {self.table_name} 
                       WHERE id >= {start_row} AND embedding_id = {epoch} % total_embeddings LIMIT {batch_size}""")
        
        blobbed_embeddings = cursor.fetchall()

        embeddings = [pickle.loads(blob) for _, blob in blobbed_embeddings]
    
        last_id = blobbed_embeddings[-1][0] if len(blobbed_embeddings) > 0 else start_row
        
        return last_id, embeddings
    
    def getPersonEmbeddingsBatch(self, start_id: int, batch_size: int) -> list[tuple[int, int, list[list[int]]]]:
        cursor = self.conn.cursor()

        cursor.execute(f"""SELECT person_id, total_embeddings, embedding FROM {self.table_name} 
                       WHERE person_id >= ? AND person_id < ? ORDER BY person_id ASC""", (start_id, start_id + batch_size))
        
        results = cursor.fetchall()

        people_embeddings = []
        
        # group a person's embeddings
        for res in results: 
            if len(people_embeddings) == 0 or people_embeddings[-1][0] != res[0]:
                people_embeddings.append([res[0], res[1], []])
            
            people_embeddings[-1][2].append(pickle.loads(res[2]))
             
        return people_embeddings
    

    def getAllEmbeddings(self) -> list[list[int]]:
        cursor = self.conn.cursor()

        cursor.execute(f'SELECT embedding FROM {self.table_name}')

        return [pickle.loads(e[0]) for e in cursor.fetchall()]

           