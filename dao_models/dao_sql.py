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
        self.insert_buffer = pd.DataFrame(columns=self.get_column_names())

        self.additional_rules = {
            # If exam type is generated to be 0 (theoretical exam) then we dont need a vehicle so set it to Null 
            # (can't be None because all such values identicate that it needs to be generated)
            "Vehicle": lambda content: None if content["Type"] else sqlalchemy.null()
        }
    
    def get_column_names(self) -> Dict[str, None]:
        # with self.engine.connect() as conn:
        #     result = conn.execute(sqlalchemy.select(self.data_object))
        return {col.name: None for col in self.data_object.columns}
    
    def generate_entry(self) -> Dict[str, Any]:
        entry = self.get_column_names()
        for column in entry.keys():
            # Check for any additional rules
            if column in self.additional_rules.keys():
                entry[column] = self.additional_rules[column](entry)
            # Check if column has been already filled
            if entry[column] is None:
                # If column is not a foreign key
                if column not in self.dependency:
                    entry[column] = BasicRuleBook.generate_column_value(column)
                else:
                    # Reflect dependend table
                    table = sqlalchemy.Table(column, self.metadata, autoload_with=self.engine)
                    table_primary_key = [col.name for col in table.primary_key.columns]
                    with self.engine.connect() as conn:
                        # Old 
                        # stmt = table.select().with_only_columns(*[table.c[key] for key in table_primary_key])
                        # result = conn.execute(stmt)
                        # random_result = random.choice(result.fetchall())[0]

                        # New
                        stmt = table.select().order_by(sqlalchemy.func.newid()).limit(1).with_only_columns(*[table.c[key] for key in table_primary_key])
                        random_result = conn.execute(stmt).first()[0]
                    entry[column] = random_result
        return entry
    
    def generate(self, number_of_entries):
        self.generated = True
        for _ in range(number_of_entries):
            contents = self.generate_entry()
            self.insert_buffer = pd.concat([self.insert_buffer, pd.DataFrame([contents])], ignore_index=True)
            with self.engine.connect() as conn:
                # print("Adding to table ", self.name)
                stmt = self.data_object.insert().values(contents)
                # You can print insert statement by uncommenting this
                # to_save = stmt.compile(dialect=sqlalchemy.dialects.mssql.dialect(), compile_kwargs={"literal_binds": True})
                conn.execute(stmt)
                conn.commit()

    def load(self, path: str) -> None:
        # Load all inserts
        inserts = pd.read_csv(path)
        # Iterate through all rows to insert
        for row in range(len(inserts)):
            with self.engine.connect() as conn:
                # take row from inserts, cut first column (index), transform it to dict
                row_insert = inserts.iloc[row].to_dict()
                self.insert_buffer = pd.concat([self.insert_buffer, pd.DataFrame([row_insert])], ignore_index=True)
                for column in row_insert.keys():
                    if column in self.additional_rules.keys():
                        row_insert[column] = self.additional_rules[column](row_insert)
                stmt = self.data_object.insert().values(row_insert)
                conn.execute(stmt)
                conn.commit()
        self.loaded = True
        print(self.name, " loaded from file")

    def save(self, path: Optional[str] = None) -> None:
        if path:
            self.insert_buffer.to_csv(f"{path}/{self.name}_insert.csv", index=False)
        else: 
            self.insert_buffer.to_csv(f"{self.name}_insert.csv", index=False)