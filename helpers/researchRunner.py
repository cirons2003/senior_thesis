## Status_codes 
UNINITIALIZED = 0
INITIALIZING_TABLES = 1
GENERATING_EMBEDDINGS = 2
CLUSTERING = 3
CLASSIFYING = 4

import sqlite3

from .setup import initialize_trial_tables
from .metadata import MetaData
from .persons import Persons
from .embeddings import Embeddings
from .prototypes import *


class ResearchRunner():
    def __init__(self, database_path, trial_name, chunking_agent, embedding_agent, clustering_agent):
        self.__database_path: str = database_path
        self.__chunking_agent: ChunkingAgent = chunking_agent
        self.__embedding_agent: EmbeddingAgent = embedding_agent
        self.__clustering_agent: ClusteringAgent = clustering_agent
        
        self.conn = sqlite3.connect(self.__database_path)

        self

        self.metadata: MetaData = MetaData(self.conn, trial_name)
        self.__current_stage: int = self.metadata.get_current_stage()
        self.__current_index: int = self.metadata.get_current_index()

        self.persons: Persons = Persons(self.conn)
        self.embeddings: Embeddings = Embeddings(self.conn, self.get_embedding_table_name())
        
    
    
    def __generateEmbeddings(self):
        assert(self.__current_stage == GENERATING_EMBEDDINGS)

        batch_size = 1000

        currIndex = self.__current_index
        while(True):
            batch = self.persons.getDescriptionBatch(currIndex, batch_size)

            if (len(batch) == 0):
                break 

            for row, desc in batch:
                assert(row == currIndex)

                chunks = self.__chunking_agent.chunk(desc)

                for chunk in chunks: 
                    embedding = self.__embedding_agent.embed(chunk)
                    self.embeddings.insertEmbedding(currIndex, embedding)

                currIndex += 1
            self.metadata.update_current_index(currIndex)
            self.conn.commit()
            self.__current_index = currIndex

        self.metadata.update_current_stage(self.__current_stage + 1)
        self.conn.commit()
        self.__current_stage += 1

    





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
                
    
            



        