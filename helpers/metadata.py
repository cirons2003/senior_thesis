"""
CREATE TABLE IF NOT EXISTS metadata (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    trial_name TEXT UNIQUE NOT NULL
    current_stage INTEGER DEFAULT 0 
    current_index INTEGER DEFAULT 0 
    embeddings_table_name TEXT NOT NULL
    centers_table_name TEXT NOT NULL
    results_table_name TEXT NOT NULL
)
"""

import sqlite3

## Throws errors
class MetaData():
    def __init__(self, conn: sqlite3.Connection, trial_name: str) -> None:
        self.__trial_name = trial_name
        self.__conn = conn

    def __select_row(self, row): 
        cursor = self.conn.cursor()
        cursor.execute(f"SELECT {row} FROM metadata WHERE trial_name = {self.__trial_name}")
        return cursor.fetchone()
    
    def get_current_stage(self) -> int:
        return self.__select_row("current_stage")

    def get_current_index(self) -> int:
        return self.__select_row("current_index")

    def get_embeddings_table_name(self) -> str: 
        return self.__select_row("embeddings_table_name")

    def get_centers_table_name(self) -> str: 
        return self.__select_row("centers_table_name")
    
    def get_results_table_name(self) -> str: 
        return self.__select_row("results_table_name")
    
    def update_current_index(self, newIndex: int) -> None:
        cursor = self.conn.cursor()

        cursor.execute(f"UPDATE metadata SET current_index = {newIndex} WHERE trial_name = {self.__trial_name}")

    def update_current_stage(self, new_stage: int): 
        cursor = self.conn.cursor()

        ## Ensure we aren't skipping stages
        cursor.execute(f"SELECT current_stage FROM metadata WHERE trial_name = {self.trial_name}")
        curr_stage = cursor.fetchone()
        assert(new_stage == curr_stage + 1)

        cursor.execute(f"UPDATE metadata SET current_stage = {new_stage} WHERE trial_name = {self.__trial_name}")

