import sqlite3
from enum import Enum
import random
from typing import List

from .metadata import MetaData
from .persons import Persons
from .embeddings import Embeddings
from .results import Results
from .prototypes import ChunkingAgent, EmbeddingAgent, ClusteringAgent
from .setup import initialize_embeddings_table, initialize_centers_table, initialize_results_table, initialize_index_table
from .inverseIndex import InverseIndex
from .updatePrinter import UpdatePrinter
from .stopwatch import Stopwatch 
from collections import Counter
import datetime
from IPython.display import display, update_display, Markdown
from .utils.chunkDescriptionBatch import chunk_description_batch


def printUpdate(message: str, indent: int = 1): 
    css = f'style="font-family: Courier; margin:2px; padding-left:{20 * indent}px; line-height:1;"'
    text = f'<pre {css}>{message}</pre>'
    display(Markdown(text))

def printToLog(message: str, indent: int = 1): 
    printUpdate(message, indent)
    """Write a log message to the file with a timestamp."""
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S,%f")[:23]
    log_entry = f"{timestamp} - INFO - {message}\n"
    
    with open('research_runner.log', "a") as log_file:
        log_file.write(log_entry)

class Stage(Enum):
    INITIALIZED = 0
    GENERATING_EMBEDDINGS = 1
    LEARNING_TOPICS = 2
    COMPUTING_RESULTS = 3
    INDEXING = 4
    DONE = 5

class ResearchRunner:
    metadata: MetaData
    persons: Persons
    embeddings: Embeddings
    results: Results

    def __init__(self, db_path: str, trial_name: str, chunking_agent: ChunkingAgent, embedding_agent: EmbeddingAgent, clustering_agent: ClusteringAgent):
        self.error = None

        # Initialize core components
        self.chunking_agent = chunking_agent
        self.embedding_agent = embedding_agent
        self.clustering_agent = clustering_agent
        self.db_path = db_path
        self.trial_name = trial_name

        printToLog('✅ ResearchRunner Initialized!')

    def __setup(self):
        # Database connection setup
        self.conn: sqlite3.Connection = sqlite3.connect(self.db_path)
        self.conn.execute("PRAGMA foreign_keys = ON;")
        self.conn.execute("PRAGMA journal_mode = WAL;")
        self.conn.execute("PRAGMA cache_size = -2000;")

        # Ensure core tables exist 
        initialize_embeddings_table(self.conn, self._get_embedding_table_name())
        #initialize_centers_table(self.conn, self._get_centers_table_name())
        initialize_results_table(self.conn, self._get_results_table_name())
        initialize_index_table(self.conn, self._get_index_table_name())

        # Connect db helpers 
        self.metadata = MetaData(self.conn, self.trial_name)
        
        self.persons = Persons(self.conn)

        self.embeddings = Embeddings(self.conn, self._get_embedding_table_name())
        if self.metadata.get_embeddings_table_name() != '':
            self.metadata.update_embeddings_table_name(self._get_embedding_table_name())

        #self.centers = Centers(self.conn, self._get_centers_table_name(), clustering_agent.topic_count)

        self.results = Results(self.conn, self._get_results_table_name())
        if self.metadata.get_results_table_name() != '':
            self.metadata.update_results_table_name(self._get_results_table_name())

        self.inverseIndex = InverseIndex(self.conn, self._get_index_table_name())
        if self.metadata.get_index_table_name() != '':
            self.metadata.update_index_table_name(self._get_index_table_name())

        # Restore research state
        self.current_stage = Stage(self.metadata.get_current_stage())
        self.current_index = self.metadata.get_current_index()
        self.person_count = self.metadata.get_person_count()

        # Validate clustering agent dimensions
        self.metadata.update_topic_count(self.clustering_agent.topic_count)

        # Initialize update_printer
        self.update_printer = UpdatePrinter(100)

    def __cleanup(self):
        if self.conn: 
            self.conn.rollback()
            self.conn.close()


    def _get_embedding_table_name(self):
        return f"embeddings_{self.chunking_agent.name}_{self.embedding_agent.name}"

    #def _get_centers_table_name(self):
    #    return f"centers_{self.chunking_agent.name}_{self.embedding_agent.name}_{self.clustering_agent.name}"

    def _get_results_table_name(self):
        return f"results_{self.chunking_agent.name}_{self.embedding_agent.name}_{self.clustering_agent.name}"

    def _get_index_table_name(self):
        return f"indexes_{self.chunking_agent.name}_{self.embedding_agent.name}_{self.clustering_agent.name}"

    def _generate_embeddings(self):
        batch_size = 100
        self.update_printer.reload(2)
        self.update_printer.update_message_level('🚀 Generating Embeddings...', 0)

        stopwatch = Stopwatch()
        start_index = self.current_index
        while True:
            batch = self.persons.getDescriptionBatch(self.current_index, batch_size)
            if not batch:
                break
            
            self.update_printer.update_message_level(f'⏳ Generating Embeddings for persons {self.current_index}-{self.current_index + batch_size - 1}...', 1)
            pids, sizes, eids, chunks = chunk_description_batch(self.chunking_agent, batch)
            embeddings = self.embedding_agent.embed(chunks)
            self.embeddings.insertEmbeddings(pids, sizes, eids, embeddings)

            #self._process_batch(batch, lambda person: self._generate_embeddings_for_person(*person), update_log)
            self.current_index += len(batch)
            self.metadata.update_current_index(self.current_index)
            self.conn.commit()
            self.update_printer.update_message_level(f"✅ Embeddings generated for persons {start_index}-{self.current_index-1}! (⏳ {stopwatch.measure()})", 0)

        self.update_printer.release_levels(1)
        self.update_printer.update_message_level(f"✅ Finished generating embeddings! ({self.current_index} results in ⏳ {stopwatch.measure()})", 0)
        self.update_printer.finish()
        # Update metadata and move to the next stage
        self.person_count = self.current_index
        self.metadata.update_person_count(self.person_count)
        self._advance_stage()

    def _train_clusters(self):
        start_time = datetime.datetime.now()
        if not self.clustering_agent.is_finished_training():
            self.clustering_agent.pass_embeddings(self.embeddings)
            self.clustering_agent.train()
        delta = datetime.datetime.now() - start_time
        printUpdate(f"✅ Topics learned! (⏳ {delta})", 2)
        self._advance_stage()


    def _compute_results_for_person(self, person_id: int, embeddings: List[List[int]]):
        result = self.clustering_agent.generate_result(embeddings)
        self.results.insertResult(person_id, result)

    def _compute_results(self):
        batch_size = 1000
        
        stopwatch = Stopwatch()
        self.update_printer.reload(2)
        self.update_printer.update_message_level("🎯 Restoring progress...", 0)
        start_index = self.current_index

        while True:
            batch = self.embeddings.getPersonEmbeddingsBatch(self.current_index, batch_size)
            if not batch:
                break

            for i, (person_id, _, embeddings) in enumerate(batch):
                self.update_printer.update_message_level(f"✅ Results generated for persons {start_index}-{self.current_index-1}! (⏳ {stopwatch.measure()})")
                self.update_printer.update_message_level(f"⏳ Generating results for persons {self.current_index}-{self.current_index + batch_size - 1} ({i}/{len(batch)})...", 1)
                result = self.clustering_agent.generate_result(embeddings)
                self.results.insertResult(person_id, result)
            
            self.current_index += len(batch)
            self.metadata.update_current_index(self.current_index)
            self.conn.commit()
    
        self.update_printer.release_levels(1)
        self.update_printer.update_message_level(f"✅ Finished! (⏳ {stopwatch.measure()})", 0)

        # Update metadata and move to the next stage
        self._advance_stage()

    def _inverse_index(self):
        """
        Generates the inverse index for the results stored in the results table.
        """
        batch_size = 100

        if self.current_stage != Stage.INDEXING:
            return

        stopwatch = Stopwatch()
        self.update_printer.reload(2)
        self.update_printer.update_message_level("🎯 Restoring progress...", 0)
        start_index = self.current_index
        
        while True:
            # Fetch a batch of results for processing
            batch = self.results.getPersonResultsBatch(self.current_index, batch_size)
            if not batch:
                break

            # Process each person's results
            for i, (person_id, result_vector) in enumerate(batch):
                self.update_printer.update_message_level(f"✅ Inverse Indexes applied to persons {start_index}-{self.current_index-1}! (⏳ {stopwatch.measure()})")
                self.update_printer.update_message_level(f"⏳ Applying inverse indexes to persons {self.current_index}-{self.current_index + batch_size - 1} ({i}/{len(batch)})...", 1)
                # Get the topic IDs (indices of 1s in the sparse binary vector)
                topic_ids = [idx for idx, val in enumerate(result_vector) if val == 1]
                
                # Add the result vector to the inverse index
                self.inverseIndex.add_result_vector(topic_ids, person_id, len(topic_ids))

            # Update the current index and commit progress
            self.current_index += len(batch)
            self.metadata.update_current_index(self.current_index)
            self.conn.commit()

        self.update_printer.release_levels(1)
        self.update_printer.update_message_level(f"✅ Finished! (⏳ {stopwatch.measure()})", 0)    

        # Update metadata to advance to the next stage
        self._advance_stage()

    def _table_exists(self, table_name: str) -> bool:
        """Checks if a table exists in the database."""
        query = f"SELECT 1 from {table_name} LIMIT 1"
        return self.conn.execute(query).fetchone() != None

    def _advance_stage(self): 
        self.current_stage = Stage(self.current_stage.value + 1)
        self.metadata.update_current_stage(self.current_stage.value)
        self.current_index = 0
        self.metadata.update_current_index(self.current_index)
        self.conn.commit()          

    def run_research(self):
        try:
            ## Connect to db & restore state 
            self.__setup()

            # Print main status 
            if self.current_stage == Stage.INITIALIZED:
                printToLog("🚀 Starting Research Pipeline...", 0)
                self._advance_stage()
            else: 
                printToLog("🚀 Resuming Research Pipeline...", 0)


            ## Generate Embeddings 
            printToLog("🏋️‍♂️ Generating Embeddings", 1)
            if self.current_stage == Stage.GENERATING_EMBEDDINGS:
                self._generate_embeddings()
            else: 
                printToLog("✅ Already done! Skipping...", 2)

            ## Train Clusters
            printToLog("📖 Learning topics...", 1)
            if self.current_stage == Stage.LEARNING_TOPICS:
                self._train_clusters()
            else: 
                printToLog("✅ Already done! Skipping...", 2)

            ## Generate Results
            printToLog("🖊️ Generating Results...", 1)
            if self.current_stage == Stage.COMPUTING_RESULTS:
                self._compute_results()
            else: 
                printToLog("✅ Already done! Skipping...", 2)

            ## Index documents
            printToLog("📁 Indexing Documents...", 1)
            if self.current_stage == Stage.INDEXING:
                self._inverse_index()
            else: 
                printToLog("✅ Already done! Skipping...", 2)
            
            ## Done!
            if self.current_stage != Stage.DONE:
                raise AssertionError(f"Stages out of sync! Current Stage: {self.current_stage}")
            printToLog("🎉 Research Completed!", 0)
        finally: 
            self.__cleanup()

    def query(self, query: str): 
        if self.current_stage != Stage.DONE:
            raise AssertionError('Cannot query until research is finished')
        
        query = query.strip()
        if not query:
            return []
        
        if not self.clustering_agent.is_finished_training():
            self.clustering_agent.train()

        chunks: list[str] = self.chunking_agent.chunk(query)
        embeddings: list[list[int]] = [self.embedding_agent.embed(c) for c in chunks]
        result: list[int] = self.clustering_agent.generate_result(embeddings)
        topics = [i for i, x in enumerate(result) if x == 1]
        matches = self.inverseIndex.get_all_documents_by_topics(topics, False)
        counter = Counter(matches)
        return [e[0] for e in counter.most_common()]
