from .dao import DAO

from rule_book import BasicRuleBook

from typing import Dict, Any, Optional

import sqlalchemy
import pandas as pd
import random


class CSVDAO(DAO):
    def __init__(self, name, data_object, dependency = ..., mapping_dict: Dict[str, str] = None, 
                 /, engine: Optional[sqlalchemy.Engine] = None, metadata: Optional[sqlalchemy.MetaData] = None):
        super().__init__(name, data_object, dependency)
        self.mapping = mapping_dict
        self.engine = engine
        self.metadata = metadata
    
    def get_column_names(self) -> Dict[str, None]:
        return {col: None for col in self.data_object.columns}
    
    def generate_entry(self, index: Optional[int] = -1) -> Dict[str, Any]:
        """Function generating one entry in storage

        To correctly assign database columns to csv file, one must change mapping dictionary
        
        {"name_from_databse": "name_in_csv"}

        {"name_in_csv": "name_from_databse"}
        
        Only if column names do not match with each other (e.g. License_ID in Examiner DB is supposed to be in Examiner_license in Examiners CSV)
        Also keep in mind that dependencies are loaded backwards, so that from current example, Reservation.csv is supposed to have name, surname, pesel of Candidate DB,
        but also Examiner_licence from Examiner DB. Notice that Examiner DB also has 

        Args:
            index Optional[int]: specifies the ith row of a db to take, if -1 then do randomly. Default: -1

        Returns:
            Dict[str, Any]: _description_
        """
        entry = self.get_column_names()
        for dep in self.dependency:
            table = sqlalchemy.Table(dep, self.metadata, autoload_with=self.engine)
            with self.engine.connect() as conn:
                stmt = table.select()
                result = conn.execute(stmt).fetchall()
                if index == -1:
                    result = random.choice(result)
                else:
                    result = result[index]
            for (csv, db) in self.mapping[dep].values():
                entry[csv] = result[db]
        
        for column in entry.keys():
            if not entry[column]:
                entry[column] = BasicRuleBook.generate_column_value(column_name=column)
        return entry

    def generate(self, number_of_entries):
        self.generated = True
        for idx in range(number_of_entries):
            contents = self.generate_entry(idx)
            self.insert_buffer = pd.concat([self.insert_buffer, pd.DataFrame(contents.items())], ignore_index=True)
            
    
    def save(self) -> None:
        self.data_object.to_csv(f"{self.name}")
