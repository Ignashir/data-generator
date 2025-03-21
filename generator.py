import pandas as pd
import sqlalchemy
import json
import os
from dotenv import load_dotenv
from typing import Optional, Dict

from db_model import create_table
from dao import DAO

load_dotenv()

class DataGenerator:
    def __init__(self, data_config_filepath: str):
        # This is intended to be a of such structure:
        # {'name_of_the_storage': object_representing_the_storage}
        # Such that it is possible to access the storage by name
        # Example:
        # {'ExaminerSheet': pd.DataFrame,
        # 'Candidate': sqlalchemy.Table}
        self.data_storage = {}
        self.load_json_config(data_config_filepath)
    
    def load_json_config(self, filepath: str):
        # Load the json file with the configuration
        with open(filepath, "r") as file:
            file_content = json.load(file)
        # Iterate over the tables in the configuration
        engine = sqlalchemy.create_engine(os.getenv("SQL_URL"))
        metadata = sqlalchemy.MetaData()
        for table_name, table_config in file_content["Tables"].items():
            print(f"Creating DAO for {table_name}")
            self.data_storage[table_name] = DAO(table_name, create_table(table_name, table_config, metadata=metadata), table_config["dependency"])
        metadata.create_all(engine)
        for sheet_name, sheet_columns in file_content["Sheets"].items():
            print(f"Creating DAO for {sheet_name}")
            self.data_storage[sheet_name] = DAO(sheet_name, pd.DataFrame(columns=sheet_columns["columns"]), sheet_columns["dependency"])
    
    def save_to_file(self, sql_filename: Optional[str] = "DataGenerator/data/create.sql") -> None:
        """Function to save all data sources to files

        Args:
            sql_filename (Optional[str], optional): Filename for all creates of tables from .json. Defaults to "DataGenerator/data/create.sql".
        """        
        with open(sql_filename, "w") as file:
            for table_name, table in self.data_storage.items():
                if isinstance(table, pd.DataFrame):
                    print(f"Saving {table_name} to file")
                    table.to_csv(f"DataGenerator/data/{table_name}.csv", index=False)
                else:
                    print(f"Saving {table_name} to file")
                    file.write(str(sqlalchemy.schema.CreateTable(table).compile()))
    
    def validate_dict(self, generate_dict: Dict[str, int]) -> None:
        """This function should be modified by any one who wants to use the DataGenerator.
        Here should be the validation of the amount of data that should be generated for each table.
        We want this validation to ensure that all data is synchronized.

        In this case it is suited for my purposes.

        Args:
            generate_dict (Dict[str, int]): Dictionary with the amount of data to be generated for each
            table.
        """
        if generate_dict["Examiner"] > generate_dict["Examiners"]:
            raise ValueError("There cannot be more (current)Examiners than (all)Examiners")
        if generate_dict["Candidate"] > generate_dict["Reservations"]:
            raise ValueError("There cannot be more Candidates than Reservations")
        if generate_dict["Exam"] > generate_dict["Reservations"]:
            raise ValueError("There cannot be more Exams than Reservations")

    def generate_data(self, generate_dict: Dict[str, int]) -> None:
        """Function to queue DAOs in a correct way (sorted by dependency)

        Args:
            generate_dict (Dict[str, int]): Dict consisting of DAOs names and number of entries to generate for them
        """
        try:
            # Validate the data to be generated
            self.validate_dict(generate_dict)
            # Sort the order of generation by the dependency
            generation_order = sorted(list(self.data_storage.values()), key=lambda x: len(x.dependency))
            idx = 0
            change = False
            # Iterate through generation order until all data storages have been generated
            while True:
                data_access = generation_order[idx]
                # Check if the data has already been generated
                # Check if this data storage is either:
                #        not dependent on any other data storage
                #        all dependencies have been already generated
                if not data_access.has_been_generated() and (data_access.is_not_dependent() or data_access.is_dependency_fulfilled(generation_order)):
                    data_access.generate(generate_dict[data_access.name])
                    change = True
                # Check if there are still operations performed
                if not change:
                    break
                idx += 1
                # We are out of bounds
                if idx >= len(generation_order):
                    # Reset index to iterate through whole list again
                    idx = 0
                    # Set change flag to false to know when there are no more operations performed -> all data storages have been generated
                    change = False
        except ValueError:
            print("Data to be generated is not valid")


sample = {
    "Examiner": 10,
    "Candidate": 10,
    "Vehicle": 5,
    "Exam": 20,
    "Reservations": 30,
    "Examiners": 10
}

DataGenerator("DataGenerator/tables.json").generate_data(sample)