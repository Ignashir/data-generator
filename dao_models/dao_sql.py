from .dao import DAO
from rule_book import BasicRuleBook
from typing import Optional, List, Any, Dict

from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData
from sqlalchemy.dialects.mssql import INTEGER
from sqlalchemy import Identity

import sqlalchemy
import pandas as pd
import csv
import random

class SQLDAO(DAO):
    def __init__(self, name: str, data_object: sqlalchemy.Table, dependency: ..., 
                 /, engine: Optional[sqlalchemy.Engine] = None, metadata: Optional[sqlalchemy.MetaData] = None):
        super().__init__(name, data_object, dependency)
        self.engine = engine
        self.metadata = metadata
        self.additional_rules_columns = {
            # If exam type is generated to be 0 (theoretical exam) then we dont need a vehicle so set it to Null 
            # (can't be None because all such values identicate that it needs to be generated)
            "Vehicle": lambda content: None if content["Type"] is False else content["Vehicle"]
        }
        self.type_changer = {
            "Gearbox": lambda content: True if content["Gearbox"] == "True" else False,
            "Result": lambda content: True if content["Result"] == "True" else False,
            "Type": lambda content: True if content["Type"] == "True" else False,
        }
        self.additional_rules_tables = {
            # Database hard coded behaviour (like using Identity for primary key)
            "Exam": lambda content: content.pop("Exam_ID")
        }
        self.prim_keys = set()
    
    def get_column_names(self) -> Dict[str, None]:
        return {col.name: "None" for col in self.data_object.columns}
    
    def is_primary_key_used(self, new_primary) -> bool:
        return new_primary in self.prim_keys

    def generate_entry(self, dependency_contents: Dict[str, Any]) -> Dict[str, Any]:
        entry = self.get_column_names()
        if self.data_object.name in self.additional_rules_tables.keys():
            self.additional_rules_tables[self.data_object.name](entry)
        for column in entry.keys():
            # Check for any additional rules
            if column in self.additional_rules_columns.keys():
                entry[column] = self.additional_rules_columns[column](entry)
            # Check if column has been already filled
            if entry[column] == "None":
                # If column is not a foreign key
                if column not in self.dependency:
                    # Generate random value
                    entry[column] = BasicRuleBook.generate_column_value(column)
                else:
                    # Take random value from database
                    entry[column] = next(dependency_contents[column]["data"], [None])[0]
                    dependency_contents[column]["remaining"] -= 1
        primary_key = self.data_object.primary_key.columns[0]
        # TODO zrobic inaczej
        if (primary_key.name != "Exam_ID"):
            # Check if primary key is unique
            while (self.is_primary_key_used(entry[primary_key.name])):
                # If it is not unique, generate new one
                entry[primary_key.name] = BasicRuleBook.generate_column_value(primary_key.name)
            # Add primary key to the set of already generated primary keys
            self.prim_keys.add(entry[primary_key.name])
        return entry

    def refill_query(self, table_name: str, connector, pull_size) -> Dict[str, Any]:
        # Reflect dependency table
        table = sqlalchemy.Table(table_name, self.metadata, autoload_with=self.engine)
        # Check amount of rows
        max_index = sqlalchemy.select(sqlalchemy.func.count()).select_from(table)
        max_index = connector.execute(max_index).scalar()
        # Set pulling size to be either the wanted size or the amount of rows in the table
        pulling_size = pull_size if max_index > pull_size else max_index
        # Get all primary keys from the table
        table_primary_key = [col.name for col in table.primary_key.columns]
        # Assign random order to each row in pulled data, and pull only primary keys
        stmt = table.select().order_by(sqlalchemy.func.newid()).limit(pulling_size).with_only_columns(*[table.c[key] for key in table_primary_key])
        # Fetch all rows, place them in the memory and return the iterator of it
        ret = connector.execute(stmt).fetchall()
        return {
            "data": iter(ret),
            "remaining": pulling_size
        }

    def generate(self, number_of_entries, batch_size: int = 2000):
        self.generated = True
        batch = []
        # Dict with rows from database and amount of rows left to pull
        pulled_dependencies = {}
        # How many rows at once to pull from database
        pull_size = 5000
        with self.engine.connect() as conn:
            # Go through all dependencies and pull necessary info from db
            for dep in self.dependency:
                pulled_dependencies[dep] = self.refill_query(dep, conn, pull_size)
            for idx in range(number_of_entries):
                # Check if any dependencies have been depleted
                for dep in self.dependency:
                    # If there are no more rows to pull
                    if pulled_dependencies[dep]["remaining"] == 0:
                        # Refill the query
                        pulled_dependencies[dep] = self.refill_query(dep, conn, pull_size)
                contents = self.generate_entry(pulled_dependencies)
                batch.append(contents)
                if len(batch) >= batch_size:
                    print(self.name, " generated ", idx, " entries")
                    conn.execute(sqlalchemy.insert(self.data_object), batch)
                    conn.commit()
                    batch = []
            if batch:
                conn.execute(sqlalchemy.insert(self.data_object), batch)
                conn.commit()

# TODO Naprawic bo czyta string, a oczekuje innych typow
    def load(self, path: str, batch_size: int = 5000) -> None:
        with self.engine.connect() as conn:
            with open(path, newline="") as bulk:
                data = csv.DictReader(bulk)
                batch = []
                for row in data:
                    # Apply any necessary rules
                    for column in row.keys():
                        if column in self.additional_rules_columns.keys():
                            row[column] = self.additional_rules_columns[column](row)
                        if column in self.type_changer.keys():
                            row[column] = self.type_changer[column](row)
                    # Add row into batch
                    self.prim_keys.add(row[self.data_object.primary_key.columns[0].name])
                    batch.append(row)
                    if len(batch) >= batch_size:
                        # Bulk insert
                        conn.execute(sqlalchemy.insert(self.data_object), batch)
                        conn.commit()
                        batch = []
                if batch:
                    conn.execute(sqlalchemy.insert(self.data_object), batch)
                    conn.commit()
        batch = None
        self.loaded = True
        print(self.name, " loaded from file")


    # TODO Make 2 separate ones fow <10k rows,  >10k rows
    def save(self, path: Optional[str] = None) -> None:
        # Get header content
        columns = self.get_column_names()
        # Set correct path to save to
        filepath = f"{path}/{self.name}_insert.csv" if path else f"{self.name}_insert.csv"
        # Pull all rows from database
        with self.engine.connect().execution_options(yield_per=True) as conn:
            stmt = self.data_object.select()
            with conn.execution_options(stream_results=True, max_row_buffer=100).execute(stmt).mappings() as result:
                with open(filepath, "w", newline="") as file:
                    writer = csv.DictWriter(file, fieldnames=columns)
                    writer.writeheader()
                    writer.writerows(result)