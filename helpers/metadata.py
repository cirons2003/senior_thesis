import sqlite3

## Throws errors
class MetaData():
    def __init__(self, conn: sqlite3.Connection, trial_name: str) -> None:
        self.__trial_name = trial_name
        self.__conn = conn
        conn.cursor().execute(f"INSERT OR IGNORE INTO metadata (trial_name) VALUES (?)", (trial_name, ))
        conn.commit()
        
    def __select_col(self, col): 
        cursor = self.__conn.cursor()
        cursor.execute(f"SELECT {col} FROM metadata WHERE trial_name = ?", (self.__trial_name, ))
        return cursor.fetchone()[0]
    
    def get_current_stage(self) -> int:
        return self.__select_col("current_stage")

    def get_current_index(self) -> int:
        return self.__select_col("current_index")
    
    def get_current_epoch(self) -> int:
        return self.__select_col("current_epoch")

    def get_person_count(self) -> int:
        return self.__select_col("person_count")
    
    def get_topic_count(self) -> int:
        return self.__select_col("topic_count")
    
    def get_embeddings_table_name(self) -> str:
        return self.__select_col("embeddings_table_name")
    
    def get_results_table_name(self) -> str:
        return self.__select_col("results_table_name")
    
    def get_index_table_name(self) -> str:
        return self.__select_col("index_table_name")
    
    def update_current_index(self, new_index: int) -> None:
        assert new_index >= 0, "new_index can't be negative"

        cursor = self.__conn.cursor()

        cursor.execute(f"UPDATE metadata SET current_index = {new_index} WHERE trial_name = ?", (self.__trial_name, ))

    def update_current_stage(self, new_stage: int): 
        assert new_stage >= 0, "new_stage can't be negative"
        cursor = self.__conn.cursor()

        ## Ensure we aren't skipping stages
        cursor.execute(f"SELECT current_stage FROM metadata WHERE trial_name = ?", (self.__trial_name, ))
        curr_stage = cursor.fetchone()[0]
        assert new_stage == curr_stage + 1, 'current_stage must increment by 1'

        cursor.execute(f"UPDATE metadata SET current_stage = {new_stage} WHERE trial_name = ?", (self.__trial_name, ))

    def update_current_epoch(self, new_epoch):
        assert new_epoch >= 0, "new_epoch can't be negative"

        cursor = self.__conn.cursor()

        ## Ensure we aren't skipping epochs
        cursor.execute(f"SELECT current_epoch FROM metadata WHERE trial_name = ?", (self.__trial_name, ))
        curr_epoch = cursor.fetchone()[0]
        assert new_epoch == curr_epoch + 1, 'current_epoch must increment by 1'
        
        cursor.execute(f"UPDATE metadata SET current_epoch = ? WHERE trial_name = ?", (new_epoch, self.__trial_name, ))

    def update_person_count(self, new_person_count: int):
        assert new_person_count >= 0, "new_person_count can't be negative"
        
        cursor = self.__conn.cursor()

        cursor.execute(f"UPDATE metadata SET person_count = ? WHERE trial_name = ?", (new_person_count, self.__trial_name, ))

    def update_topic_count(self, new_topic_count: int):
        assert new_topic_count > 0, "new_topic_count must be positive"

        currVal = self.get_topic_count()
        assert currVal == 0 or new_topic_count == currVal, "Topic count is immutable"

        if currVal != 0: 
            return

        cursor = self.__conn.cursor()

        cursor.execute(f"UPDATE metadata SET topic_count = ? WHERE trial_name = ?", (new_topic_count, self.__trial_name))

    def update_embeddings_table_name(self, new_name: str): 
        cursor = self.__conn.cursor() 

        cursor.execute(f"UPDATE metadata SET embeddings_table_name = ? WHERE trial_name = ?", (new_name, self.__trial_name))

    def update_results_table_name(self, new_name: str): 
        cursor = self.__conn.cursor() 

        cursor.execute(f"UPDATE metadata SET results_table_name = ? WHERE trial_name = ?", (new_name, self.__trial_name))

    def update_index_table_name(self, new_name: str): 
        cursor = self.__conn.cursor() 

        cursor.execute(f"UPDATE metadata SET index_table_name = ? WHERE trial_name = ?", (new_name, self.__trial_name))