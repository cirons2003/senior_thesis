import sqlite3
import pickle

class Centers:
    def __init__(self, conn: sqlite3.Connection, table_name: str, topic_count: int): 
        self.__conn = conn
        self.__table_name = table_name
        self.__topic_count = topic_count

    def update_centers(self, new_centers: list[list[int]]): 
        assert len(new_centers) == self.__topic_count, "Incompatible topic_count being inserted"

        cursor = self.__conn.cursor()

        cursor.execute(f"DELETE FROM {self.__table_name}")
        for center in new_centers: 
            center_blob = pickle.dumps(center)
            cursor.execute(f"INSERT INTO {self.__table_name} (center) VALUES (?)", [center_blob])

    def get_centers(self) -> list[list[int]]: 
        cursor = self.__conn.cursor()

        cursor.execute(f"SELECT center FROM {self.__table_name}")
        blobs = cursor.fetchall()

        return [pickle.loads(blob[0]) for blob in blobs]
    

""" 
def __train_clusters(self): 
    if self.__current_stage != Stage.CLUSTERING.value: 
        return 
    
    num_epochs = 30
    batch_size = 1000

    curr_epoch = self.__current_epoch
    curr_index = self.__current_index
    while curr_epoch < num_epochs:
        while(True):
            last_id, embedding_batch = self.embeddings.getEmbeddingBatch(curr_index, batch_size, curr_epoch)
            if len(embedding_batch) == 0:
                break

            self.__clustering_agent.batch_train(embedding_batch)

            curr_index = last_id + 1
            self.metadata.update_current_index(curr_index)
            self.__current_index = curr_index
            self.conn.commit()
        
        print(f"Finished epoch {curr_epoch}.")

        curr_epoch += 1
        curr_index = 0
        self.metadata.update_current_epoch(curr_epoch)
        self.metadata.update_current_index(curr_index)
        self.__current_epoch = curr_epoch
        self.__current_index = curr_index
        self.conn.commit()
    
    self.metadata.update_current_stage(self.__current_stage + 1)
    self.metadata.update_current_index(0)
    self.metadata.update_current_epoch(0)
    self.__current_stage += 1
    self.__current_index = 0
    self.__current_epoch = 0
    self.conn.commit()
"""