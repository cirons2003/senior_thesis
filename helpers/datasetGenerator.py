import sqlite3
import pandas as pd 
from enum import Enum

from .persons import Persons
from .metadata import MetaData
from .person import generatePerson
from .setup import initialize_database_tables
from .openAi import generateDescription

##Status codes
class Stage(Enum):
    GENERATING_DESCRIPTIONS = 1
    DONE = 2


class DatasetGenerator():
    def __init__(self, database_path, csv_path: str): 
        try:
            self.error = None
            print(f"Initializing DatasetGenerator for {database_path}...")
            self.csv_path = csv_path
            
            self.conn = sqlite3.connect(database_path)
            print(f"Connected to database!")

            initialize_database_tables(self.conn)
            print(f"Initialized Database Tables!")

            self.metadata = MetaData(self.conn, "root")
            self.persons = Persons()
            print("Finished initializing, restoring progress...")

            self.current_stage = self.metadata.get_current_stage()
            self.current_index = self.metadata.get_current_index()
            if self.current_stage == Stage.GENERATING_DESCRIPTIONS.value:
                print(f"Progress restored!\n\tStage: Generating Descriptions\n\tOffset: Starting at row 5039\nUse generate_dataset to Proceed!")
            if self.current_stage == Stage.DONE.value:
                print(f"Progress restored!\n\tStage: Done\n\tOffset: N/a\nNothing left to be done!")
        except Exception as e: 
            print("ERROR : Initialization failed. Please try again.\n\t(Use flushErrors for details)")
            self.error = e
            self.cleanup()
            return

    def flush_errors(self):
        print(self.error)

    def __cleanup(self) -> None:
        if self.conn.in_transaction:
            self.conn.rollback()
        self.conn.close()


    def __generate_descriptions(self) -> None: 
        if self.current_stage != Stage.GENERATING_DESCRIPTIONS.value:
            return
        curr_index = self.current_index
        batch_size = 1000
        
        while(True):
            csv = pd.read_csv(self.csv_path, skiprows=lambda x: x < curr_index, nrows = batch_size)

            if (len(csv) == 0):
                break

            print(f"Generating Descriptions for rows {curr_index}-{curr_index + batch_size - 1}...")
            for _, row in csv.iterrows():
                person = generatePerson(row)
                llm_description = generateDescription(person.generateDescription())
                self.persons.insertDescription(curr_index, llm_description)
                curr_index += 1
            
            self.metadata.update_current_index(curr_index)
            self.conn.commit()
            self.current_index = curr_index
        
        print("Done generating descriptions!")
        self.metadata.update_current_stage(self.current_stage + 1)
        self.conn.commit()
        self.current_stage += 1
        print("Moving to next stage...")

    def generate_dataset(self) -> None: 
        if self.current_stage == Stage.DONE.value:
            print("Nothing left to do... aborting...")
            return
        
        try:
            self.__generate_descriptions()
        except Exception as e: 
            print("ERROR : Description generation failed. Please try again.\n\t(Use flushErrors for details)")
            self.error = e
            self.__cleanup()
            return 
        
        self.__cleanup()
        print("Finished generating dataset!")
        