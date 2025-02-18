import sqlite3
import pandas as pd
import json
from .persons import Persons
from .setup import initialize_database_tables

class DatasetGenerator:
    def __init__(self, database_path ): 
        try:
            print(f"Initializing DatasetGenerator for {database_path}...")

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

    def cleanup(self) -> None:
        """ Safely closes the database connection. """
        if self.conn.in_transaction:
            self.conn.rollback()
        self.conn.close()

    def extract_responses(self, jsonl_path: str) -> None: 
        """ Reads JSONL, extracts responses, and stores them in the database. """
        try:
            # Read entire JSONL file at once
            json_data = pd.read_json(jsonl_path, lines=True)

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
        
    


