import sqlite3
import pickle

class Results():
    def __init__(self, conn: sqlite3.Connection, table_name: str):
        self.__conn = conn
        self.__table_name = table_name

    
    def insertResult(self, person_id: int, result: list[int]):
        cursor = self.__conn.cursor()
        
        blob_result = pickle.dumps(result)

        cursor.execute(f"INSERT OR REPLACE INTO {self.__table_name} (id, person_id, result) VALUES ({person_id}, {person_id}, {blob_result})")