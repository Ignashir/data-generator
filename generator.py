import pandas as pd
import sqlalchemy
import json
import os
import traceback
import argparse
from dotenv import load_dotenv
from typing import Optional, Dict, Self

from db_model import create_table
from dao_models.dao_csv import CSVDAO
from dao_models.dao_sql import SQLDAO

load_dotenv()

class DataGenerator:
    def __init__(self, data_config_filepath: str, path: Optional[str] = "data/snapshots/"):
        # This is intended to be a of such structure:
        # {'name_of_the_storage': object_representing_the_storage}
        # Such that it is possible to access the storage by name
        # Example:
        # {'ExaminerSheet': pd.DataFrame,
        # 'Candidate': sqlalchemy.Table}
        # What is the most current snapshot
        path_cwd = os.path.join(os.getcwd(), path)
        self.current_version = len(os.listdir(path_cwd))
        self.path_to_save = os.path.join(path_cwd, f"T{self.current_version+1}")
        self.data_storage = {}
        self.load_json_config(os.path.join(os.getcwd(), data_config_filepath))
    
    def load_json_config(self, filepath: str):
        # Load the json file with the configuration
        with open(filepath, "r") as file:
            file_content = json.load(file)
        # Iterate over the tables in the configuration
        engine = sqlalchemy.create_engine(os.getenv("SQL_URL"))
        metadata = sqlalchemy.MetaData()
        for table_name, table_config in file_content["Tables"].items():
            # print(f"Creating DAO for {table_name}")
            self.data_storage[table_name] = SQLDAO(table_name, create_table(table_name, table_config, metadata=metadata), 
                                                list(table_config["foreign_key"]), engine=engine, metadata=metadata)
        metadata.create_all(engine)
        for sheet_name, sheet_columns in file_content["Sheets"].items():
            # print(f"Creating DAO for {sheet_name}")
            self.data_storage[sheet_name] = CSVDAO(sheet_name, pd.DataFrame(columns=sheet_columns["columns"]), 
                                                   list(sheet_columns["foreign_key"]), sheet_columns["foreign_key"], engine=engine, metadata=metadata, snapshot=self.path_to_save)
    
    def create_loading_dict(self, path: str) -> Dict[str, str]:
        return {file.split("_")[0]: os.path.join(path, file) for file in os.listdir(path)}

    # TODO check if this works properly with many time spaces
    def load_from_folder(self, folder_path: str = "data/snapshots/") -> Self:
        folder_path = os.path.join(os.getcwd(), folder_path)
        # Iterate through all snapshots in folder
        for timestamp in os.listdir(folder_path):
            if not os.path.isdir(os.path.join(folder_path, timestamp)):
                continue
            files = self.create_loading_dict(os.path.join(folder_path, timestamp))
            # Sort the order of generation by the dependency
            loading_order = sorted(list(self.data_storage.values()), key=lambda x: len(x.dependency))
            idx = 0
            # Keep track of how many storages have been loaded
            loaded = 0
            # Iterate through loading order until all data storages have been loaded
            while loaded < len(loading_order):
                data_access = loading_order[idx]
                # Check if the data has already been loaded
                # Check if this data storage is either:
                #        not dependent on any other data storage
                #        all dependencies have been already loaded
                if not data_access.has_been_loaded() and (data_access.is_not_dependent() or data_access.is_dependency_fulfilled_for_loading(loading_order)):
                    data_access.load(files[data_access.name])
                    loaded += 1
                idx += 1
                # We are out of bounds
                if idx >= len(loading_order):
                    # Reset index to iterate through whole list again
                    idx = 0
            [ds.unload() for ds in loading_order]
        return self

    def save_to_file(self, sql_filename: Optional[str] = "data/create.sql") -> None:
        """Function to save all data sources to files

        Args:
            sql_filename (Optional[str], optional): Filename for all creates of tables from .json. Defaults to "DataGenerator/data/create.sql".
        """
        sql_filename = os.path.join(os.getcwd(), sql_filename)
        # Save Tables into Create SQL statements        
        with open(sql_filename, "w") as file:
            for table_name, table in self.data_storage.items():
                if isinstance(table, SQLDAO):
                    print(f"Saving {table_name} to file")
                    file.write(str(sqlalchemy.schema.CreateTable(table.data_object).compile()))
        os.makedirs(self.path_to_save, exist_ok=True)
        # Save Sheets into .csv and Insert values into .csv
        for data in self.data_storage.values():
            data.save(self.path_to_save)
    
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
            raise ValueError("There cannot be more current Examiners (db) than all Examiners (csv)")
        if generate_dict["Candidate"] > generate_dict["Reservations"]:
            raise ValueError("There cannot be more Candidates than Reservations")
        if generate_dict["Exam"] > generate_dict["Reservations"]:
            raise ValueError("There cannot be more Exams than Reservations")

    def generate_data(self, generate_dict: Dict[str, int]) -> Self:
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
            # Keep track of how many storages have been generated
            generated = 0
            # Iterate through generation order until all data storages have been generated
            while generated < len(generation_order):
                data_access = generation_order[idx]
                # Check if the data has already been generated
                # Check if this data storage is either:
                #        not dependent on any other data storage
                #        all dependencies have been already generated
                if not data_access.has_been_generated() and (data_access.is_not_dependent() or data_access.is_dependency_fulfilled(generation_order)):
                    data_access.generate(generate_dict[data_access.name])
                    generated += 1
                idx += 1
                # We are out of bounds
                if idx >= len(generation_order):
                    # Reset index to iterate through whole list again
                    idx = 0
            return self
        except ValueError:
            traceback.print_exc()


def from_nothing(path_to_config: str) -> None:
    generation_dict = {}
    print("===============================")
    print("Provide amount of data to be generated")
    generation_dict["Examiner"] = int(input("Examiner : "))
    generation_dict["Candidate"] = int(input("Candidate : "))
    generation_dict["Vehicle"] = int(input("Vehicle : "))
    generation_dict["Exam"] = int(input("Exam : "))
    generation_dict["Reservations"] = int(input("Reservations : "))
    generation_dict["Examiners"] = int(input("Examiners : "))
    print("===============================")
    DataGenerator(path_to_config).generate_data(generation_dict).save_to_file()


def preloaded(path_to_config: str) -> None:
    generation_dict = {}
    print("===============================")
    print("Provide amount of data to be generated on top of previous data")
    generation_dict["Examiner"] = int(input("Examiner : "))
    generation_dict["Candidate"] = int(input("Candidate : "))
    generation_dict["Vehicle"] = int(input("Vehicle : "))
    generation_dict["Exam"] = int(input("Exam : "))
    generation_dict["Reservations"] = int(input("Reservations : "))
    generation_dict["Examiners"] = int(input("Examiners : "))
    print("===============================")
    DataGenerator(path_to_config).load_from_folder().generate_data(generation_dict).save_to_file()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generation of data for provided structures specified in .json file")
    parser.add_argument("--config", type=str, default="tables.json", help="Path to .json file with all configuration, for more information on structure, please refer to README")
    parser.add_argument("--load", action="store_true", help="Boolean value that specifies if you want to generate from nothing or load already generated data. Data to be loaded should be in DataGenerator/data/snapshot/")

    args = parser.parse_args()
    if args.load:
        preloaded(args.config)
    else:
        from_nothing(args.config)