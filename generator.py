import pandas as pd
import sqlalchemy
import json

from db_model import create_table

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
        engine = sqlalchemy.create_engine("url_to_database")
        metadata = sqlalchemy.MetaData()
        for table_name, table_config in file_content["Tables"].items():
            print(f"Generating data for {table_name}")
            self.data_storage[table_name] = create_table(table_name, table_config, metadata=metadata)
            metadata.create_all(engine)
        for sheet_name, sheet_columns in file_content["Sheets"].items():
            print(f"Generating data for {sheet_name}")
            self.data_storage[sheet_name] = pd.DataFrame(columns=sheet_columns)