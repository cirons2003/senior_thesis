import sqlite3
import pandas as pd
import json
from .persons import Persons
from .setup import initialize_database_tables

class DatasetGenerator:
    def __init__(self, database_path, jsonl_path: str): 
        try:
            print(f"Initializing DatasetGenerator for {database_path}...")

            self.jsonl_path = jsonl_path
            self.conn = sqlite3.connect(database_path)
            print(f"Connected to database!")

            initialize_database_tables(self.conn)
            print(f"Initialized Database Tables!")

            self.persons = Persons(self.conn)
            print("Finished initialization!")
        except Exception as e: 
            print(f"❌ ERROR: Initialization failed. {e}")
            self.conn.close()
            raise

    def __cleanup(self) -> None:
        """ Safely closes the database connection. """
        if self.conn.in_transaction:
            self.conn.rollback()
        self.conn.close()

    def extract_responses(self) -> None: 
        """ Reads JSONL, extracts responses, and stores them in the database. """
        try:
            # Read entire JSONL file at once
            json_data = pd.read_json(self.jsonl_path, lines=True)

            if json_data.empty:
                print("⚠️ No data found in JSONL file.")
                return

            print(f"📤 Extracting {len(json_data)} responses...")

            for _, row in json_data.iterrows():
                try:
                    response_body = row["response"]["body"]
                    content = response_body["choices"][0]["message"]["content"]
                    custom_id  = row["custom_id"]
                    index = int(custom_id.split('-')[1])
                    self.persons.insertDescription(index, content)
                except (KeyError, IndexError, TypeError):
                    print(f"⚠️ Error processing row {index}, skipping...")
                    continue  # Skip faulty rows
            
            self.conn.commit()
            print("✅ Done extracting responses!")
        except Exception as e:
            print(f"❌ ERROR: Failed to process JSON file. {e}")
        finally:
            self.__cleanup()
            print("🚀 Finished!")





'''import sqlite3
import pandas as pd
import json
from enum import Enum
from .persons import Persons
from .metadata import MetaData
from .person import generatePerson
from .setup import initialize_database_tables

## Status codes
class Stage(Enum):
    EXTRACTING_RESPONSES = 1
    DONE = 2

class DatasetGenerator():
    def __init__(self, database_path, jsonl_path: str): 
        try:
            self.error = None
            print(f"Initializing DatasetGenerator for {database_path}...")

            self.jsonl_path = jsonl_path
            self.conn = sqlite3.connect(database_path)
            print(f"Connected to database!")

            initialize_database_tables(self.conn)
            print(f"Initialized Database Tables!")

            self.metadata = MetaData(self.conn, "root")
            self.persons = Persons()
            print("Finished initializing, restoring progress...")

            self.current_stage = self.metadata.get_current_stage()
            self.current_index = self.metadata.get_current_index()
            if self.current_stage == Stage.EXTRACTING_RESPONSES.value:
                print(f"Progress restored!\n\tStage: Extracting Responses\n\tOffset: Starting at row {self.current_index}\nUse generate_dataset to Proceed!")
            if self.current_stage == Stage.DONE.value:
                print(f"Progress restored!\n\tStage: Done\n\tOffset: N/A\nNothing left to be done!")
        except Exception as e: 
            print("ERROR: Initialization failed. Please try again.\n\t(Use flushErrors for details)")
            self.error = e
            self.__cleanup()
            return

    def flush_errors(self):
        print(self.error)

    def __cleanup(self) -> None:
        if self.conn.in_transaction:
            self.conn.rollback()
        self.conn.close()

    def __extract_responses(self) -> None: 
        if self.current_stage != Stage.EXTRACTING_RESPONSES.value:
            return
        
        curr_index = self.current_index
        batch_size = 1000
        
        while True:
            try:
                # Read JSONL file in chunks
                json_data = pd.read_json(self.jsonl_path, lines=True, skiprows=curr_index, nrows=batch_size)

                if json_data.empty:
                    break

                print(f"Extracting Responses for rows {curr_index}-{curr_index + batch_size - 1}...")

                for _, row in json_data.iterrows():
                    try:
                        response_body = row["response"]["body"]
                        content = response_body["choices"][0]["message"]["content"]
                        self.persons.insertDescription(curr_index, content)
                        curr_index += 1
                    except (KeyError, IndexError, TypeError):
                        print(f"❌ Error processing row {curr_index}, skipping...")
                        continue  # Skip faulty rows
            
                self.metadata.update_current_index(curr_index)
                self.conn.commit()
                self.current_index = curr_index
            
            except Exception as e:
                print(f"❌ ERROR: Failed to process JSON file. Details: {e}")
                break
        
        print("✅ Done extracting responses!")
        self.metadata.update_current_stage(Stage.DONE.value)
        self.conn.commit()
        self.current_stage = Stage.DONE.value
        print("🚀 Moving to next stage...")

    def generate_dataset(self) -> None: 
        if self.current_stage == Stage.DONE.value:
            print("Nothing left to do... aborting...")
            return
        
        try:
            self.__extract_responses()
        except Exception as e: 
            print("ERROR: Response extraction failed. Please try again.\n\t(Use flushErrors for details)")
            self.error = e
            self.__cleanup()
            return 
        
        self.__cleanup()
        print("🎉 Finished generating dataset!")
'''