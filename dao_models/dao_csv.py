from .dao import DAO

from rule_book import BasicRuleBook

from typing import Dict, Any, Optional

import sqlalchemy
import pandas as pd

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
        # Go through all dependencies and pull necessary info from db
        for dep in self.dependency:
            # Reflect dependency Table
            table = sqlalchemy.Table(dep, self.metadata, autoload_with=self.engine)
            # Get all results from that Table
            with self.engine.connect() as conn:
                # Get amount of rows
                max_index = sqlalchemy.select(sqlalchemy.func.count()).select_from(table)
                max_index = conn.execute(max_index).scalar()
                # There is a row to take
                if index < max_index:
                    stmt = table.select().order_by(table.primary_key.columns).offset(index).limit(1)
                    result = conn.execute(stmt).first()
                else:
                    # If you want to draw a random entry from dependency database uncomment this
                    # stmt = table.select().order_by(sqlalchemy.func.newid()).limit(1)
                    # result = conn.execute(stmt).first()
                    continue
            # Map the resulted list onto a dict with column names as keys and pulled values as values
            result = {col.name: val for col, val in zip(table.columns, result)}
            # Set wanted info from pulled values onto the entry dictionary
            for (csv, db) in self.mapping[dep].items():
                entry[csv] = result[db]
        # Fill other None values keys with generated values
        for column in entry.keys():
            if not entry[column]:
                entry[column] = BasicRuleBook.generate_column_value(column)
        return entry

    def generate(self, number_of_entries):
        self.generated = True
        for idx in range(number_of_entries):
            contents = self.generate_entry(idx)
            # print("Adding to table ", self.name)
            self.data_object = pd.concat([self.data_object, pd.DataFrame([contents])], ignore_index=True)

    def load(self, path: str) -> None:
        self.data_object = pd.read_csv(path)
        self.loaded = True
        print(self.name, " loaded from file")
    
    def save(self, path: Optional[str] = None) -> None:
        filepath = f"{path}/{self.name}_sheet.csv" if path else f"{self.name}_sheet.csv"
        self.data_object.to_csv(filepath, index=False)
