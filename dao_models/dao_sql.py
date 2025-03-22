from .dao import DAO
from rule_book import BasicRuleBook
from typing import Optional, List, Any, Dict

import sqlalchemy
import pandas as pd
import random

class SQLDAO(DAO):
    def __init__(self, name: str, data_object: Any, dependency: ..., 
                 /, engine: Optional[sqlalchemy.Engine] = None, metadata: Optional[sqlalchemy.MetaData] = None):
        super().__init__(name, data_object, dependency)
        self.engine = engine
        self.metadata = metadata
        self.insert_buffer = pd.DataFrame(columns=self.get_column_names())
    
    def get_column_names(self) -> Dict[str, None]:
        with self.engine.connect() as conn:
            result = conn.execute(sqlalchemy.select(self.data_object))
        return {col: None for col in result.keys()}
    
    def generate_entry(self) -> Dict[str, Any]:
        entry = self.get_column_names()
        for column in entry.keys():
            # If column is not a foreign key
            if column not in self.dependency:
                entry[column] = BasicRuleBook.generate_column_value(column_name=column)
            else:
                # Reflect dependend table
                table = sqlalchemy.Table(column, self.metadata, autoload_with=self.engine)
                table_primary_key = [col.name for col in table.primary_key.columns]
                with self.engine.connect() as conn:
                    stmt = table.select().with_only_columns(*[table.c[key] for key in table_primary_key])
                    result = conn.execute(stmt)
                    random_result = random.choice(result.fetchall())[0]
                entry[column] = random_result
        return entry
    
    def generate(self, number_of_entries):
        self.generated = True
        for _ in range(number_of_entries):
            contents = self.generate_entry()
            self.insert_buffer = pd.concat([self.insert_buffer, pd.DataFrame(contents.items())], ignore_index=True)
            with self.engine.connect() as conn:
                print("Adding to table ", self.name)
                stmt = self.data_object.insert().values(contents)
                # You can print insert statement by uncommenting this
                # to_save = stmt.compile(dialect=sqlalchemy.dialects.mssql.dialect(), compile_kwargs={"literal_binds": True})
                conn.execute(stmt)
                conn.commit()
    
    def save(self) -> None:
        self.insert_buffer.to_csv(f"{self.name}_insert.csv")