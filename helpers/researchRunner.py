import sqlite3
from enum import Enum
import random

from .metadata import MetaData
from .persons import Persons
from .embeddings import Embeddings
from .prototypes import *
from .setup import initialize_embeddings_table, initialize_centers_table, initialize_results_table

class Stage(Enum):
    GENERATING_EMBEDDINGS = 1
    CLUSTERING = 2
    CLASSIFYING = 3
    DONE = 4

class ResearchRunner():
    def __init__(self, database_path, trial_name, chunking_agent, embedding_agent, clustering_agent):
        self.__database_path: str = database_path
        self.__chunking_agent: ChunkingAgent = chunking_agent
        self.__embedding_agent: EmbeddingAgent = embedding_agent
        self.__clustering_agent: ClusteringAgent = clustering_agent
        
        self.conn = None
        self.__initializeConnection()

        initialize_embeddings_table(self.conn, self.get_embedding_table_name())
        initialize_centers_table(self.conn, self.get_centers_table_name())
        initialize_results_table(self.conn, self.get_results_table_name())

        self.metadata: MetaData = MetaData(self.conn, trial_name)
        self.__current_stage: int = self.metadata.get_current_stage()
        self.__current_index: int = self.metadata.get_current_index()
        self.__current_epoch: int = self.metadata.get_current_epoch()

        self.persons: Persons = Persons(self.conn)
        self.embeddings: Embeddings = Embeddings(self.conn, self.get_embedding_table_name())
        
    def __initializeConnection(self): 
        self.conn = sqlite3.connect(self.__database_path)
        self.conn.execute("PRAGMA foreign_keys = ON;")
        self.conn.execute("PRAGMA journal_mode = WAL;")  
        self.conn.execute("PRAGMA synchronous = NORMAL;")  
        self.conn.execute("PRAGMA cache_size = -2000;")
    
    def __closeConnection(self): 
        if self.conn.in_transaction:
            self.conn.rollback()
        self.conn.close()
        self.conn = None
    
    def __generateEmbeddings(self):
        if self.__current_stage != Stage.GENERATING_EMBEDDINGS.value:
            return

        batch_size = 1000

        currIndex = self.__current_index
        while(True):
            batch = self.persons.getDescriptionBatch(currIndex, batch_size)

            if (len(batch) == 0):
                break 

            for row, desc in batch:
                assert(row == currIndex)

                chunks = self.__chunking_agent.chunk(desc)
                total_embeddings = len(chunks)
                embedding_id = 0
                random.shuffle(chunks) #Eliminate positional correlations

                for chunk in chunks: 
                    embedding = self.__embedding_agent.embed(chunk)
                    self.embeddings.insertEmbedding(currIndex, embedding, embedding_id, total_embeddings)
                    embedding_id += 1

                currIndex += 1
            self.metadata.update_current_index(currIndex)
            self.conn.commit()
            self.__current_index = currIndex

        self.metadata.update_current_stage(self.__current_stage + 1)
        self.metadata.update_current_index(0)
        self.conn.commit()
        self.__current_stage += 1
        self.__current_index = 0
        

    def __trainClusters(self): 
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
                
            curr_epoch += 1
            curr_index = 0
            self.metadata.update_current_epoch(curr_epoch)
            self.metadata.update_current_index(curr_index)
            self.__current_epoch = curr_epoch
            self.__current_index = curr_index
            self.commit()
        
        self.metadata.update_current_stage(self.__current_stage + 1)
        self.metadata.update_current_index(0)
        self.metadata.update_current_epoch(0)
        self.__current_stage += 1
        self.__current_index = 0
        self.__current_epoch = 0
        self.commit()
                
    def


    def get_embedding_table_name(self): 
        chunking_approach = self.__chunking_agent.name
        embedding_approach = self.__embedding_agent.name

        return f"embeddings_{chunking_approach}_{embedding_approach}"

    def get_centers_table_name(self): 
        chunking_approach = self.__chunking_agent.name
        embedding_approach = self.__embedding_agent.name
        clustering_approach = self.__clustering_agent.name

        return f"centers_{chunking_approach}_{embedding_approach}_{clustering_approach}"

    def get_results_table_name(self): 
        chunking_approach = self.__chunking_agent.name
        embedding_approach = self.__embedding_agent.name
        clustering_approach = self.__clustering_agent.name

        return f"results_{chunking_approach}_{embedding_approach}_{clustering_approach}"
                
    
         



        