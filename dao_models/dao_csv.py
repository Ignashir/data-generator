from .dao import DAO

from rule_book import BasicRuleBook

from typing import Dict, Any, Optional

import sqlalchemy
import pandas as pd
import csv

class CSVDAO(DAO):
    def __init__(self, name, data_object, dependency = ..., mapping_dict: Dict[str, str] = None, 
                 /, engine: Optional[sqlalchemy.Engine] = None, metadata: Optional[sqlalchemy.MetaData] = None,
                 keep_object: Optional[bool] = False):
        super().__init__(name, data_object, dependency)
        self.mapping = mapping_dict
        self.engine = engine
        self.metadata = metadata
        self.keep = keep_object
    
    def get_column_names(self) -> Dict[str, None]:
        return {col: None for col in self.data_object.columns}
    
    def generate_entry(self, dependency_dict: Dict[str, Any] = None, index: Optional[int] = -1) -> Dict[str, Any]:
        """Function generating one entry in storage

        To correctly assign database columns to csv file, one must change mapping dictionary
        
        {"name_in_csv": "name_from_databse"}
        
        Only if column names do not match with each other (e.g. License_ID in Examiner DB is supposed to be in Examiner_license in Examiners CSV)
        Also keep in mind that dependencies are loaded backwards, so that from current example, Reservation.csv is supposed to have name, surname, pesel of Candidate DB,
        but also Examiner_licence from Examiner DB. Notice that Examiner DB also has 

        Args:
            dependency_dict (Dict[str, Any], optional): Dictionary containing all dependencies. Defaults to None.
            index (int, optional): Index of the entry. Defaults to -1.

        Returns:
            Dict[str, Any]: _description_
        """
        entry = self.get_column_names()
        # Go through all dependencies and pull necessary info from db
        for dep in self.dependency:
            # Reflect dependency Table
            # table = sqlalchemy.Table(dep, self.metadata, autoload_with=self.engine)
            # # Get all results from that Table
            # with self.engine.connect() as conn:
            #     # Get amount of rows
            #     max_index = sqlalchemy.select(sqlalchemy.func.count()).select_from(table)
            #     max_index = conn.execute(max_index).scalar()
            #     # There is a row to take
            #     if index < max_index:
            #         stmt = table.select().order_by(table.primary_key.columns).offset(index).limit(1)
            #         result = conn.execute(stmt).mappings().first()
            #     else:
            #         # If you want to draw a random entry from dependency database uncomment this
            #         # stmt = table.select().order_by(sqlalchemy.func.newid()).limit(1)
            #         # result = conn.execute(stmt).first()
            #         continue
            result = next(dependency_dict[dep]["data"], None)
            dependency_dict[dep]["remaining"] -= 1
            if result is None:
                continue
            # Set wanted info from pulled values onto the entry dictionary
            for (csv, db) in self.mapping[dep].items():
                entry[csv] = result[db]
        # Fill other None values keys with generated values
        for column in entry.keys():
            if not entry[column]:
                entry[column] = BasicRuleBook.generate_column_value(column)
        return entry

    # TODO jezeli chce podtrzymac losowanie wierszy to musi to byc tutaj, jedna opcja to po prostu select druga to select z newid
    def refill_query(self, table_name: str, connector, pull_size: int, current_place: int) -> Dict[str, Any]:
        # Reflect dependency table
        table = sqlalchemy.Table(table_name, self.metadata, autoload_with=self.engine)
        # Check amount of rows
        max_index = sqlalchemy.select(sqlalchemy.func.count()).select_from(table)
        max_index = connector.execute(max_index).scalar()
        # Set pulling size to be either the wanted size or the amount of rows in the table
        pulling_size = pull_size if max_index > (pull_size + current_place) else (max_index - current_place)
        # If there is no more rows to pull return a "dummy" object
        if pulling_size <= 0:
            return {
                "data": iter([]),
                "remaining": 0
            }
        # Assign random order to each row in pulled data, and pull only primary keys
        stmt = table.select().order_by(table.primary_key.columns).offset(current_place).limit(pulling_size).with_only_columns(*[table.c[key] for key in self.mapping[table_name].values()])
        ret = connector.execute(stmt).mappings().fetchall()
        return {
            "data": iter(ret),
            "remaining": pulling_size
        }

    def generate(self, number_of_entries, batch_size: int = 5000):
        self.generated = True
        batch = []
        pulled_dependencies = {}
        pull_size = 100
        with self.engine.connect() as conn:
            with open(f"DataGenerator/data/snapshots/{self.name}.csv", "w") as file:
                writer = csv.DictWriter(file, fieldnames=self.get_column_names().keys())
                writer.writeheader()
                for dep in self.dependency:
                        if dep not in pulled_dependencies:
                            pulled_dependencies[dep] = self.refill_query(dep, conn, pull_size, 0)
                for idx in range(number_of_entries):
                    for dep in self.dependency:
                        if pulled_dependencies[dep]["remaining"] == 0:
                            pulled_dependencies[dep] = self.refill_query(dep, conn, pull_size, idx)
                    contents = self.generate_entry(pulled_dependencies, idx)
                    batch.append(contents)
                    if len(batch) >= batch_size:
                        # writer.writerows(batch)
                        print(self.name, " is saving to file ", idx)
                        batch = []
                # if batch:
                #     writer.writerows(batch)
        if self.keep:
            self.data_object = pd.read_csv("DataGenerator/data/snapshots/Examiner.csv")
            # print("Adding to table ", self.name)
            # self.data_object = pd.concat([self.data_object, pd.DataFrame([contents])], ignore_index=True)

    def load(self, path: str) -> None:
        self.data_object = pd.read_csv(path)
        self.loaded = True
        print(self.name, " loaded from file")
    
    def save(self, path: Optional[str] = None) -> None:
        filepath = f"{path}/{self.name}_sheet.csv" if path else f"{self.name}_sheet.csv"
        self.data_object.to_csv(filepath, index=False)
