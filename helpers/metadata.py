import sqlite3

## Throws errors
class MetaData():
    def __init__(self, conn: sqlite3.Connection, trial_name: str) -> None:
        self.__trial_name = trial_name
        self.__conn = conn
        conn.cursor().execute(f"INSERT OR IGNORE INTO metadata (trial_name) VALUES ({trial_name})")
        conn.commit()
        
    def __select_row(self, row): 
        cursor = self.__conn.cursor()
        cursor.execute(f"SELECT {row} FROM metadata WHERE trial_name = {self.__trial_name}")
        return cursor.fetchone()
    
    def get_current_stage(self) -> int:
        return self.__select_row("current_stage")

    def get_current_index(self) -> int:
        return self.__select_row("current_index")
    
    def get_current_epoch(self) -> int:
        return self.__select_row("current_epoch")

    def get_person_count(self) -> int:
        return self.__select_row("person_count")
    
    def update_current_index(self, newIndex: int) -> None:
        cursor = self.__conn.cursor()

        cursor.execute(f"UPDATE metadata SET current_index = {newIndex} WHERE trial_name = {self.__trial_name}")

    def update_current_stage(self, new_stage: int): 
        cursor = self.__conn.cursor()

        ## Ensure we aren't skipping stages
        cursor.execute(f"SELECT current_stage FROM metadata WHERE trial_name = {self.__trial_name}")
        curr_stage = cursor.fetchone()
        assert(new_stage == curr_stage + 1)

        cursor.execute(f"UPDATE metadata SET current_stage = {new_stage} WHERE trial_name = {self.__trial_name}")

    def update_current_epoch(self, new_epoch):
        cursor = self.__conn.cursor()

        ## Ensure we aren't skipping epochs
        cursor.execute(f"SELECT current_epoch FROM metadata WHERE trial_name = {self.__trial_name}")
        curr_stage = cursor.fetchone()
        assert(new_epoch == curr_stage + 1)\
        
        cursor.execute(f"UPDATE metadata SET current_epoch = {new_epoch} WHERE trial_name = {self.__trial_name}")

    def update_person_count(self, new_person_count: int):
        cursor = self.__conn.cursor()

        cursor.execute(f"UPDATE metadata SET person_count = {new_person_count} WHERE trial_name = {self.__trial_name}")