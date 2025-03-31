from .dao import DAO
from rule_book import BasicRuleBook
from typing import Optional, List, Any, Dict

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
        self.additional_rules_tables = {
            # Database hard coded behaviour (like using Identity for primary key)
            "Exam": lambda content: content.pop("Exam_ID")
        }
    
    def get_column_names(self) -> Dict[str, None]:
        return {col.name: "None" for col in self.data_object.columns}
    
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
        pull_size = 1000
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

# TODO tu cos nie gralo z tego co pamietam
    def load(self, path: str, batch_size: int = 1000) -> None:
        with self.engine.connect() as conn:
            with open(path, newline="") as bulk:
                data = csv.DictReader(bulk)
                batch = []
                for row in data:
                    # Apply any necessary rules
                    for column in row.keys():
                        if column in self.additional_rules.keys():
                            row[column] = self.additional_rules[column](row)
                    # Add row into batch
                    batch.append(row)
                    if len(batch) >= batch_size:
                        # Bulk insert
                        conn.execute(sqlalchemy.insert(self.data_object), batch)
                        conn.commit()
                        batch = []
                if batch:
                    conn.execute(sqlalchemy.insert(self.data_object), batch)
                    conn.commit()
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