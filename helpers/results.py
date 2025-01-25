import sqlite3
import pickle

class Results():
    def __init__(self, conn: sqlite3.Connection, table_name: str):
        self.__conn = conn
        self.__table_name = table_name

    
    def insertResult(self, person_id: int, result: list[int]):
        cursor = self.__conn.cursor()
        
        blob_result = pickle.dumps(result)

        cursor.execute(f"INSERT OR REPLACE INTO {self.__table_name} (id, person_id, result) VALUES (?, ?, ?)", (person_id, person_id, blob_result))

    def getAllResults(self): 
        cursor = self.__conn.cursor()
        
        cursor.execute(f'SELECT result FROM {self.__table_name}')

        return [pickle.loads(r[0]) for r in cursor.fetchall()]

    def getPersonResultsBatch(self, start_id: int, batch_size: int) -> list[tuple[int, list[int]]]:
        """
        Fetches a batch of result vectors from the results table.

        Args:
            start_id (int): Starting person ID for the batch.
            batch_size (int): Number of results to fetch.

        Returns:
            list[tuple[int, list[int]]]: List of tuples (person_id, result_vector).
        """
        cursor = self.__conn.cursor()

        # Query to fetch person IDs and serialized results
        query = f"""
        SELECT person_id, result
        FROM {self.__table_name}
        WHERE person_id >= ? 
        ORDER BY person_id ASC
        LIMIT ?
        """
        cursor.execute(query, (start_id, batch_size))
        results = cursor.fetchall()

        # Deserialize the result vectors
        return [(row[0], pickle.loads(row[1])) for row in results]


