import sqlalchemy
import random
import pandas as pd
from typing import List, Dict, Self, Any, Union, Optional
from abc import ABC, abstractmethod

import sqlalchemy.dialects
import sqlalchemy.dialects.mssql

from rule_book import BasicRuleBook


class DAO(ABC):
    def __init__(self, name: str, data_object: Any, dependency: List[str] = []):
        self.name = name
        self.data_object = data_object
        self.dependency = dependency
        self.generated = False
    
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
    
    @abstractmethod
    def get_column_names(self) -> dict:
        """ Extract column names from certain storage and transform it to look like this : {"column1": None (None will be replaced by generated value)}
        Returns:
            dict: Dict with keys as names of columns and values as None
        """
        pass

    @abstractmethod
    def generate_entry(self) -> None:
        pass

    @abstractmethod
    def generate(self, number_of_entries: int) -> None:
        pass
    
    @abstractmethod
    def save(self, path: Optional[str] = None) -> None:
        pass
            