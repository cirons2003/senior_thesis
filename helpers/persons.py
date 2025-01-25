import sqlite3

## throws errors
class Persons(): 
    def __init__(self, conn: sqlite3.Connection) -> None:   
        self.conn = conn 

    def insertDescription(self, person_id: int, description: str) -> None:
        cursor = self.conn.cursor()
        cursor.execute(f"INSERT OR REPLACE INTO persons (id, description) VALUES (?, ?)", (person_id, description, ))

    def getDescriptionBatch(self, row_index: int, batch_size: int) -> list[list]:
        cursor = self.conn.cursor()
        cursor.execute(f"SELECT id, description FROM persons WHERE id >= {row_index} LIMIT {batch_size}")
        return cursor.fetchall() 