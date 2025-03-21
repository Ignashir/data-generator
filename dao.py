import sqlalchemy
import pandas as pd
from typing import List, Dict, Self, Any, Union, Optional

from rule_book import BasicRuleBook

class DAO:
    def __init__(self, name: str, data_object: Any, dependency: List[str] = [], /, engine: Optional[sqlalchemy.Engine] = None):
        self.name = name
        self.data_object = data_object
        self.dependency = dependency
        self.generated = False
        self.engine = engine
    
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
        for column in entry.keys():
            # If column is not a foreign key
            if column not in self.dependency:
                entry[column] = BasicRuleBook.generate_column_value(column_name=column)
            else:
                pass
        return entry

    def generate(self, number_of_entries: int) -> None:
        self.generated = True
        contents = self.generate_entry()
        print(contents)
        # for _ in range(number_of_entries):
        #     self.generate_entry()