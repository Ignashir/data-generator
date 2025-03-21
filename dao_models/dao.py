import sqlalchemy
import random
import pandas as pd
from typing import List, Dict, Self, Any, Union, Optional

import sqlalchemy.dialects
import sqlalchemy.dialects.mssql

from rule_book import BasicRuleBook


# TODO jak juz bedzie dzialac to zmienic zeby byly klasy dzidziczace kazda na poszczegolny typ storage
class DAO:
    def __init__(self, name: str, data_object: Any, dependency: List[str] = [], 
                 /, engine: Optional[sqlalchemy.Engine] = None, metadata: Optional[sqlalchemy.MetaData] = None):
        self.name = name
        self.data_object = data_object
        self.dependency = dependency
        self.generated = False
        self.engine = engine
        self.metadata = metadata
        self.insert_buffer = pd.DataFrame(columns=self.get_column_names()) if isinstance(self.data_object, sqlalchemy.Table) else None
    
    def is_dependent_on(self, other: Self) -> bool:
        return other.name in self.dependency
    
    def is_dependency_fulfilled(self, list_of_generated: List[Self]) -> bool:
        """Function to check if all dependencies has been already generated

        Args:
            list_of_generated (List[Self]): List of previously generated data storages

        Returns:
            bool: Return True if all dependencies have been created, False otherwise
        """
        return all([True if data_storage.has_been_generated() else False for data_storage in list_of_generated if self.is_dependent_on(data_storage)])

    def has_been_generated(self) -> bool:
        return self.generated
    
    def is_not_dependent(self) -> bool:
        return not self.dependency
    
    def get_column_names(self) -> dict:
        # Match data object to correctly extract column names
        # If needed add more types (e.g. custom class) and provide code to extrack dict in such form
        # {
        # "column1": None (None will be replaced by generated value)
        # }
        match type(self.data_object):
            case sqlalchemy.Table:
                with self.engine.connect() as conn:
                    result = conn.execute(sqlalchemy.select(self.data_object))
                return {col: None for col in result.keys()}
            case pd.DataFrame:
                return {col: None for col in self.data_object.columns}

    def generate_entry(self) -> None:
        entry = self.get_column_names()
        # DB generation
        if isinstance(self.data_object, sqlalchemy.Table):
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
        else:
            pass

    def generate(self, number_of_entries: int) -> None:
        self.generated = True
        for _ in range(number_of_entries):
            contents = self.generate_entry()
            self.insert_buffer = pd.concat([self.insert_buffer, pd.DataFrame(contents)], ignore_index=True)
            with self.engine.connect() as conn:
                print("Adding to table ", self.name)
                stmt = self.data_object.insert().values(contents)
                # You can print insert statement by uncommenting this
                # to_save = stmt.compile(dialect=sqlalchemy.dialects.mssql.dialect(), compile_kwargs={"literal_binds": True})
                conn.execute(stmt)
                conn.commit()
    
    def save(self) -> None:
        match isinstance(self.data_object):
            case sqlalchemy.Table:
                self.insert_buffer.to_csv(f"{self.name}_insert")
            case pd.DataFrame:
                self.data_object.to_csv(f"{self.name}")
            