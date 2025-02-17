import sqlite3
from enum import Enum
import random
from typing import List
import logging

from .metadata import MetaData
from .persons import Persons
from .embeddings import Embeddings
from .results import Results
from .prototypes import ChunkingAgent, EmbeddingAgent, ClusteringAgent
from .setup import initialize_embeddings_table, initialize_centers_table, initialize_results_table, initialize_index_table
from .inverseIndex import InverseIndex
from collections import Counter

logging.basicConfig(
    filename="research_runner.log", 
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

class Stage(Enum):
    GENERATING_EMBEDDINGS = 0
    COMPUTING_RESULTS = 1
    INDEXING = 2
    DONE = 3

class ResearchRunner:
    metadata: MetaData
    persons: Persons
    embeddings: Embeddings
    results: Results

    def __init__(self, conn: sqlite3.Connection, trial_name: str, chunking_agent: ChunkingAgent, embedding_agent: EmbeddingAgent, clustering_agent: ClusteringAgent):
        self.error = None


        # Initialize core components
        self.chunking_agent = chunking_agent
        self.embedding_agent = embedding_agent
        self.clustering_agent = clustering_agent

        # Database connection setup
        self.conn = conn
        self.conn.execute("PRAGMA foreign_keys = ON;")
        self.conn.execute("PRAGMA journal_mode = WAL;")
        self.conn.execute("PRAGMA cache_size = -2000;")

        # Initialize tables and metadata
        initialize_embeddings_table(self.conn, self._get_embedding_table_name())
        #initialize_centers_table(self.conn, self._get_centers_table_name())
        initialize_results_table(self.conn, self._get_results_table_name())
        initialize_index_table(self.conn, self._get_index_table_name())

        self.metadata = MetaData(self.conn, trial_name)
        self.persons = Persons(self.conn)
        self.embeddings = Embeddings(self.conn, self._get_embedding_table_name())
        if self.metadata.get_embeddings_table_name() != '':
            self.metadata.update_embeddings_table_name(self._get_embedding_table_name())
        #self.centers = Centers(self.conn, self._get_centers_table_name(), clustering_agent.topic_count)
        self.results = Results(self.conn, self._get_results_table_name())
        if self.metadata.get_results_table_name() != '':
            self.metadata.update_results_table_name(self._get_results_table_name())

        self.inverseIndex = InverseIndex(conn, self._get_index_table_name())
        if self.metadata.get_index_table_name() != '':
            self.metadata.update_index_table_name(self._get_index_table_name())

        self.current_stage = Stage(self.metadata.get_current_stage())
        self.current_index = self.metadata.get_current_index()
        self.person_count = self.metadata.get_person_count()

        # Validate clustering agent dimensions
        self.metadata.update_topic_count(clustering_agent.topic_count)

    def _get_embedding_table_name(self):
        return f"embeddings_{self.chunking_agent.name}_{self.embedding_agent.name}"

    #def _get_centers_table_name(self):
    #    return f"centers_{self.chunking_agent.name}_{self.embedding_agent.name}_{self.clustering_agent.name}"

    def _get_results_table_name(self):
        return f"results_{self.chunking_agent.name}_{self.embedding_agent.name}_{self.clustering_agent.name}"

    def _get_index_table_name(self):
        return f"indexes_{self.chunking_agent.name}_{self.embedding_agent.name}_{self.clustering_agent.name}"

    def _process_batch(self, batch: List[tuple], process_func):
        """Generic batch processor."""
        for item in batch:
            process_func(item)

    def _generate_embeddings_for_person(self, person_id: int, description: str):
        chunks = self.chunking_agent.chunk(description)
        random.shuffle(chunks)  # Eliminate positional correlations

        for idx, chunk in enumerate(chunks):
            embedding = self.embedding_agent.embed(chunk)
            self.embeddings.insertEmbedding(person_id, embedding, idx, len(chunks))

    def _generate_embeddings(self):
        batch_size = 1000
        while True:
            batch = self.persons.getDescriptionBatch(self.current_index, batch_size)
            if not batch:
                break

            self._process_batch(batch, lambda person: self._generate_embeddings_for_person(*person))
            self.current_index += len(batch)
            self.metadata.update_current_index(self.current_index)
            self.conn.commit()

        # Update metadata and move to the next stage
        self.person_count = self.current_index
        self.metadata.update_person_count(self.person_count)
        self._advance_stage()

    def _train_clusters(self):
        if not self.clustering_agent.is_finished_training():
            self.clustering_agent.pass_embeddings(self.embeddings)
            self.clustering_agent.train()

    def _compute_results_for_person(self, person_id: int, embeddings: List[List[int]]):
        result = self.clustering_agent.generate_result(embeddings)
        self.results.insertResult(person_id, result)

    def _compute_results(self):
        batch_size = 100
        while True:
            batch = self.embeddings.getPersonEmbeddingsBatch(self.current_index, batch_size)
            if not batch:
                break
            
            self._process_batch(batch, lambda person: self._compute_results_for_person(person[0], person[2]))
            self.current_index += len(batch)
            self.metadata.update_current_index(self.current_index)
            self.conn.commit()

        # Update metadata and move to the next stage
        self._advance_stage()

    def _inverse_index(self):
        """
        Generates the inverse index for the results stored in the results table.
        """
        if self.current_stage != Stage.INDEXING:
            return

        batch_size = 100
        while True:
            # Fetch a batch of results for processing
            batch = self.results.getPersonResultsBatch(self.current_index, batch_size)
            if not batch:
                break

            # Process each person's results
            for person_id, result_vector in batch:
                # Get the topic IDs (indices of 1s in the sparse binary vector)
                topic_ids = [idx for idx, val in enumerate(result_vector) if val == 1]
                
                # Add the result vector to the inverse index
                self.inverseIndex.add_result_vector(topic_ids, person_id, len(topic_ids))

            # Update the current index and commit progress
            self.current_index += len(batch)
            self.metadata.update_current_index(self.current_index)
            self.conn.commit()

        # Update metadata to advance to the next stage
        self._advance_stage()

    def _table_exists(self, table_name: str) -> bool:
        """Checks if a table exists in the database."""
        query = f"SELECT 1 from {table_name} LIMIT 1"
        return self.conn.execute(query).fetchone() != None
        
    def _advance_stage(self):
        """Moves to the next stage, checking for reusable tables on the first transition."""
        if self.current_stage == Stage.GENERATING_EMBEDDINGS:
            if self._table_exists(self._get_embedding_table_name()):
                logging.info(f"✅ Reusing existing embeddings table: {self._get_embedding_table_name()}")
                print(f"✅ Reusing existing embeddings table: {self._get_embedding_table_name()}")
            else:
                self._generate_embeddings()
        
        if self.current_stage == Stage.COMPUTING_RESULTS:
            if self._table_exists(self._get_results_table_name()):
                logging.info(f"✅ Reusing existing results table: {self._get_results_table_name()}")
                print(f"✅ Reusing existing results table: {self._get_results_table_name()}")
            else:
                self._train_clusters()
                self._compute_results()

        if self.current_stage == Stage.INDEXING:
            if self._table_exists(self._get_index_table_name()):
                logging.info(f"✅ Reusing existing index table: {self._get_index_table_name()}")
                print(f"✅ Reusing existing index table: {self._get_index_table_name()}")
            else:
                self._inverse_index()

        # Move to the next stage
        self.current_stage = Stage(self.current_stage.value + 1)
        self.metadata.update_current_stage(self.current_stage.value)
        self.current_index = 0
        self.metadata.update_current_index(self.current_index)
        self.conn.commit()


    def run_research(self):
        logging.info("🚀 Starting Research Pipeline...")
        print("🚀 Starting Research Pipeline...")
        self._advance_stage()
        logging.info("✅ Research Completed!")
        print("✅ Research Completed!")

    def query(self, query: str): 
        query = query.strip()
        if not query:
            return []
        chunks: list[str] = self.chunking_agent.chunk(query)
        embeddings: list[list[int]] = [self.embedding_agent.embed(c) for c in chunks]
        result: list[int] = self.clustering_agent.generate_result(embeddings)
        topics = [i for i, x in enumerate(result) if x == 1]
        matches = self.inverseIndex.get_all_documents_by_topics(topics, False)
        counter = Counter(matches)
        return [e[0] for e in counter.most_common()]
        