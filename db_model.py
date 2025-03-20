import sqlalchemy
import re

# TODO Hour nie jest typem danych zmienic na DateTime

# Possible types to be converted from json to sqlalchemy types
types_dict = {
    "Integer": sqlalchemy.Integer,
    "Float": sqlalchemy.Float,
    "Varchar": sqlalchemy.String,
    "Boolean": sqlalchemy.Boolean,
    "Date": sqlalchemy.Date,
    "DateTime": sqlalchemy.DateTime,
    "Char": sqlalchemy.CHAR,
    "Text": sqlalchemy.Text
}

def return_datatype(datatype_string: str) -> sqlalchemy.types.TypeEngine:
    """Converts a string datatype to a sqlalchemy datatype

    Args:
        datatype_string (str): String taken from the json configuration file, representing desired datatype and possible arguments

    Raises:
        Exception: When desired datatype is not supported -> not in types_dict, to fix add the datatype to the dictionary

    Returns:
        sqlalchemy.types.TypeEngine: sqlalchemy datatype corresponding to the string
    """
    # Check if the datatype is supported
    if (dtype := re.split(r"\(", datatype_string)[0]) not in types_dict.keys():
        raise Exception(f"Datatype {dtype} not supported")
    # Ensure proper work with / without arguments
    try:
        # Extract the datatype and the arguments
        datatype, args = datatype_string.split("(")
        args = [int(arg) for arg in args[:-1].split(",")]
        # Return the datatype with the arguments
        return types_dict[datatype](*args)
    except ValueError:
        # Return the datatype without the arguments
        datatype = datatype_string
        return types_dict[datatype]()

    

def create_table(table_name: str, table_contents: dict, /, metadata: sqlalchemy.MetaData) -> sqlalchemy.Table:
    """Function that creates a sqlalchemy table based on the provided configuration

    Args:
        table_name (str): Name provided as a key to its json configuration file
        table_contents (dict): Attributes provided as a value to the key in the json configuration file
        metadata (sqlalchemy.MetaData): _description_

    Returns:
        sqlalchemy.Table: _description_
    """    
    table = sqlalchemy.Table(table_name, 
                             metadata,
                             *[
                                sqlalchemy.Column(column_name, return_datatype(column_type), primary_key=(column_name in table_contents["primary_key"])) 
                                for column_name, column_type 
                                in table_contents["attr"].items()
                             ] + [
                                sqlalchemy.Column(column_name, return_datatype(column_type), sqlalchemy.ForeignKey(column_ref)) 
                                for column_name, (column_type, column_ref)
                                in table_contents["foreign_key"].items()
                             ])
    return table
