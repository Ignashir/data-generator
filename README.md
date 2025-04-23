# Data Generator
## This program provides a base for generating entries into many different databases + other data storages.
## In order to use it to your tasks please make sure that all points below are provided
<li> Configuration file (.json) - Prepare .json file with all your data structures described as in our example (tables.json). All database tables should be put in Tables, each having attr (attributes / columns), primary_key, foreign key. All .csv files should be put in Sheets, having columns and foreign_key (Specifing what mapping should be applied onto columns from the provided database Table).
<li> Make sure that all columns present in your data structures are described in rule_book.py, if not add them as ("column_name_all_lower_no_special_char": (function_instance, index(usually -1)))
<li> Make sure that all your data types from database are also provided in db_model.py. If not, add them as ("column_name_from_json": sqlalchemy_corresponding_type)

## Tailoring to your own needs
### I have tried to provide quite a flexible program, so I hope that it will be a breeze modifying it. Here are the steps necessary to provide new functionality
In case of a new data structure, you will need to:
<li> Create a new key in configuration json file, and specify there all columns, as well as dependencies on other data structures. It is important that you specify dependencies as foreign keys in the .json, similarly to what is already presented with "Sheets". <br>PS. Remember to add your structure in DataGenerator.load_json_config.
<li> For this new data structure you will need to create a new class for it. Please use provided dao.py DAO class for inheriting, because it enforces upon you necessary functions.
<li> Probably you will need to also add new mappings for columns, as described above, and maybe new data types for database also described above.

### Please note that data generated here is being added to the provided database. So there is no need to load the data to test some things

## In case of any problems, questions please feel free to make an Issue Thread or contact me.