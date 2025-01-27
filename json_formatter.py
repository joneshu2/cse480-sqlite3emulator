"""
@author Hunter Jones <joneshu2>
Summer 2023
External Resources Used:
1 python.org official documentation: https://docs.python.org/3/library/json.html#module-json
JSON Formatter for databases in Project 6
"""

from project import *


def print_table(table):
    """
    Takes a dictionary and pretty prints it line by line.
    Helper function to help with testing and viewing intermediate format.
    :param table: A dictionary of dictionary containing data from a file.
    data : dict[str, dict[str, str]]
    """
    # data = Dict { 'record' : { 'column_name' : value}}
    header = "         "
    for name in table.column_names:
        header += "[Column]" + "      "
    header = header[:-4]
    print()
    print("\nTable Name: {t}".format(t=table.name))
    print(header)
    for row in table.rows:
        complete_row = "[Row]     "
        for name, data in zip(table.column_names, list(row.data)):
            pair = name + " : "
            if data is not None:
                if isinstance(data, str):
                    pair += data
                else:
                    pair = pair + str(data)
            else:
                pair += "None"
            complete_row += pair + "    "
        print(complete_row[:-1])


def rwrite_rows(row, column_names):
    """
    Write the rows of a table to json a file
    :param row: Row object to write from.
    :param column_names: the names of the columns to pair data to.
    """
    row_schema = dict(zip(column_names, list(row.data)))
    return row_schema


def rread_json_file(filename: str):
    """
    Takes a JSON formatted file and returns a data object.
    Reads JSON files.
    :param filename: The filename denoting a Json formatted file.
    :return data: Database object containing tables and other class information.
    """

    data = None
    db_name = None
    db_tables = {}

    with open(filename, 'r', newline='', encoding="utf-8") as jsonfile:
        # Convert to python list which holds a dictionary object for each entry
        json_list = json.load(jsonfile)

        # Get the database attributes
        db_name = json_list["name"]
        db_size = json_list["size"]

        # Get attributes for tables
        for table in json_list["tables"]:
            table_name = table["name"]
            table_rows = []

            loaded_table = Table(table_name)
            loaded_table.schema = table["schema"][0]
            loaded_table.column_names = list(loaded_table.schema)
            loaded_table.default_values = {}
            loaded_table.column_size = len(loaded_table.column_names)

            # Create Row objects for a table
            for row in table["rows"]:
                entry_data = []
                for name, entry in row.items():
                    entry_data.append(entry)
                loaded_row = Row(entry_data)
                loaded_table.rows.append(loaded_row)
                loaded_table.size += 1

            # Add the table to the database
            db_tables[table_name] = loaded_table

    data = Database(db_name)
    data.tables = db_tables
    data.size = db_size

    return data


def rwrite_json_file(filename: str, data: Database):
    """
    Takes a filename(to be written to) and a data object.
    Writes JSON files.
    :param filename: The filename to be written to.
    :param data: Database object containing tables.
    """

    db_tables = data.tables

    # Create dictionary of attributes for database
    json_dict = {"name": data.name, "size": data.size, "tables": []}

    for name, table in db_tables.items():
        # Create dictionary of attributes for table
        table_dict = {"name": table.name, "schema": [table.schema]}
        row_list = []
        # Loop through rows and add row data as key value pairs in the dictionary
        for row in table.rows:
            row_result = write_rows(row, table.column_names)
            row_list.append(row_result)
        # Add list of row kv pairs to table
        table_dict["rows"] = row_list
        # Add table data to database table
        json_dict["tables"].append(table_dict)

    with open(filename, 'w', newline='', encoding="utf-8") as jsonfile:
        # Convert the database dictionary into a json string and write that to our file
        json_str = json.JSONEncoder().encode(json_dict)
        jsonfile.write(json_str)


def conn_create_test(dbname):
    """ Testing control for creating a Database and queries """
    # Create Connection for table, this also creates a database with the same name
    conn = connect(dbname)
    # Execute sql statements to fill table
    conn.execute("CREATE TABLE names (name TEXT, id INTEGER);")
    conn.execute("INSERT INTO names VALUES ('James', 1), ('Yaxin', 3), ('Li', 2), (NULL, 4);")
    result1 = conn.execute("SELECT * FROM names ORDER BY id;")
    conn.execute("CREATE TABLE grades (id INTEGER, grade REAL);")
    conn.execute("INSERT INTO grades VALUES (3, 3.0);")
    conn.execute("INSERT INTO grades VALUES (1, 2.0);")
    conn.execute("INSERT INTO grades VALUES (2, 3.5);")
    result2 = conn.execute("SELECT * FROM grades ORDER BY id;")

    # ASSERT tables created correctly and queries returned correctly
    print()
    result1_list = list(result1)
    expected1 = [('James', 1), ('Li', 2), ('Yaxin', 3), (None, 4)]
    assert expected1 == result1_list
    result2_list = list(result2)
    expected2 = [(1, 2.0), (2, 3.5), (3, 3.0)]
    assert expected2 == result2_list
    print()

    return conn


def json_read_test(conn, input_filename):
    """ Testing control for reading a json file into a Database object"""

    #conn_db = conn.database

    db = rread_json_file(input_filename)  # Return database object

    return


def json_write_test(conn, output_filename):
    """ Testing control for writing a Database object to a json file"""
    conn_db = conn.database
    db_tables = conn_db.tables

    rwrite_json_file(output_filename, conn_db)

    return


def json_testing():
    """
    Main Testing control for json conversion between a database and file storage
    """

    # This will create a connection to add data to a table before the other tests
    db1name = "jsontest1.db"
    conn1 = conn_create_test(db1name)
    output_filename1 = "jsontest1_db_to_jsonfile"
    #json_write_test(conn1, output_filename1)
    conn1.close()

    input_filename1 = "jsontest1_db_to_jsonfile"
    conn2 = connect("jsontest1_jsonfile_to_db")
    json_read_test(conn2, input_filename1)
    conn2.close()



    return


json_testing()

""" 
Database Object
        self.name = database_name  # the name for the database
        self.size = 0  # the number of tables in the database
        self.tables = {}  # the dictionary of tables { table key : table object }
        self.joined_table = None  # the most recent joined table to be used or discarded after a transaction

Table Object
        self.name = table_name  # the name for the table
        self.schema = {}  # the schema for the table
        self.column_names = []  # the list of column names for the table
        self.default_values = {}  # the dictionary of default values for columns
        self.column_size = 0  # the number of columns for the table
        self.rows = []  # list of tuples containing the data of each row
        self.size = 0  # the number of rows for the table
Row Object
        self.data = tuple(row_data)
        self.size = len(row_data)
        self.primary_key = None
"""

# JSON Database Table Example
"""
based on test.create_database.03
filename = jsontest1.db
table 1 = names         rows = ('James', 1), ('Yaxin', 3), ('Li', 2), (NULL, 4)
table 2 = grades        rows = (3, 3.0), (1, 2.0), (2, 3.5)
rows == rows.data, only including data in json, everything else can be determined from that 

{
    "name": jsontest1.db",
    "size": 2,
    "tables": [
        {
            "name": "names",
            "schema": [
                {
                    "name": "TEXT",
                    "id": "INTEGER"
                }
            ],
            "column_names": ["name", "id"],
            "default_values": null,
            "rows": [
                {
                    "name": "James",
                    "id": 1
                },
                {
                    "name": "Yaxin",
                    "id": 3
                },
                {
                    "name": "Li",
                    "id": 2
                },
                {
                    "name": null,
                    "id": 4
                }
            ]
        },
        {
            "name": "grades",
            "schema": [
                {
                    "id": "INTEGER",
                    "grade": "REAL"
                }
            ],
            "column_names": ["id", "grade"],
            "default_values": null,
            "rows": [
                {
                    "id": 3,
                    "grade": 3.0
                },
                {
                    "id": 1,
                    "grade": 2.0
                },
                {
                    "id": 2,
                    "grade": 3.5
                }
            ]
        }
    ],
    "joined_table": null
}

"""

"""
def read_json_file(filename):
    # Takes a JSON formatted file and returns an intermediate data object.
    # Similar to read_csv_file, except works for JSON files.
    # :param filename: The filename denoting a Json formatted file.
    # :return data: Dictionary object containing data from the file.
    # Database.tables = {"table_name" : Table}
    # Table.rows = [Row, ]
    # Row.data = Tuple(row data)
    # data = Dict { 'row#' : { 'column_name' : value}}
    data = dict()

    with open(filename, 'r', newline='', encoding="utf-8") as jsonfile:
        # Convert to python list which holds a dictionary object for each entry
        json_list = json.load(jsonfile)
        # Set row0 entry as attribute name keys with None values
        data["row0"] = {attribute_name: None for attribute_name in json_list[0].keys()}
        row_id = 0

        for row in json_list:
            row_id += 1
            row_name = "row" + str(row_id)
            # Each row is already formatted as {'attribute_name' : value}
            data[row_name] = row

    return data
"""

"""
def write_json_file(filename, data):
    # Takes a filename(to be written to) and a data object
    # Writes JSON files. Similar to write_csv_file.
    # :param filename: The filename to be written to.
    # :param data: A dictionary of records from a file.

    with open(filename, 'w', newline='', encoding="utf-8") as jsonfile:
        # Delete attribute record, not needed for writing
        del data["row0"]

        json_list = []
        # Add each record to the list from our data dictionary
        for record in data.values():
            json_list.append(record)

        # Convert the list into a string and write that to our file
        json_str = json.JSONEncoder().encode(json_list)
        jsonfile.write(json_str)
"""

"""
def print_table(data_dict):
    # Takes a dictionary and pretty prints it line by line.
    # Helper function to help with testing and viewing intermediate format.
    # :param data_dict: A dictionary of dictionary containing data from a file.
    # data_dict : dict[str, dict[str, str]]
    # data = Dict { 'row#' : { 'column_name' : value}}
    for row, column_dict in data_dict.items():
        complete_row = ""
        for column, data in column_dict.items():
            if data is not None:
                pair = column + ":" + data
            else:
                pair = column
            complete_row += pair + ","
        print(row + " " + complete_row[:-1])
"""

