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

    import pickle

    def insertEmbeddings(self, person_ids: list[int], sizes: list[int], embedding_ids: list[int], embeddings: list[list[float]]) -> None:
        """
        Inserts a batch of embeddings for a person into the database.

        Args:
            person_index (int): ID of the person the embeddings belong to.
            embeddings (list[list[int]]): List of embeddings to insert.
            total_embeddings (int): Total number of embeddings for this person.
        """
        cursor = self.conn.cursor()

        # Convert embeddings to blobs
        embedding_data = [
            (person_ids[i], embedding_ids[i], sizes[i], pickle.dumps(embeddings[i]))
            for i in range(len(person_ids))
        ]

        # Use executemany() for batch insertion
        cursor.executemany(f"""
            INSERT INTO {self.table_name} (person_id, embedding_id, total_embeddings, embedding) 
            VALUES (?, ?, ?, ?)
        """, embedding_data)

        self.conn.commit()  # Commit the transaction

    
    def getEmbeddingBatch(self, start_row: int, batch_size: int, epoch: int) -> tuple[int, list[list[int]]]:
        cursor = self.conn.cursor()

        cursor.execute(f"""SELECT id, embedding FROM {self.table_name} 
                       WHERE id >= {start_row} AND embedding_id = {epoch} % total_embeddings LIMIT {batch_size}""")
        
        blobbed_embeddings = cursor.fetchall()

        embeddings = [pickle.loads(blob) for _, blob in blobbed_embeddings]
    
        last_id = blobbed_embeddings[-1][0] if len(blobbed_embeddings) > 0 else start_row
        
        return last_id, embeddings
    
    def getPersonEmbeddingsBatch(self, start_id: int, batch_size: int) -> list[tuple[int, int, list[list[int]]]]:
        """
        Gets person embeddings tuples for people between [start_id, start_id + batch_size - 1]
        Tuple of form (person_id, total_embeddings, embeddings)
        """
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

        # assert lengths check out 
        for etup in people_embeddings:
            assert etup[1] == len(etup[2])    
        
        return people_embeddings
    

    def getAllEmbeddings(self) -> list[list[int]]:
        cursor = self.conn.cursor()

        cursor.execute(f'SELECT embedding FROM {self.table_name}')

        return [pickle.loads(e[0]) for e in cursor.fetchall()]

           