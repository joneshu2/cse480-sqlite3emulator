"""
@author Hunter Jones <joneshu2>
Summer 2023
External Resources Used:
1 python.org official documentation: https://docs.python.org/3/library/json.html#module-json
2 python.org official documentation: https://docs.python.org/3/tutorial/errors.html
Module imitating sqlite3.
"""

from typing import TypeVar, Tuple, List, Set, Dict
import string
from operator import itemgetter
import copy
import json

T = TypeVar('T')
Row = TypeVar('Row')  # Row Class instance
Table = TypeVar('Table')  # Table Class instance
View = TypeVar('View')  # View Class instance
LockSystem = TypeVar("LockSystem")  # LockSystem Class instance
Database = TypeVar('Database')  # Database Class instance
Connection = TypeVar('Connection')  # Connection Class instance

_ALL_DATABASES = {}
_ALL_DATABASES_LOCKSYSTEMS = {}


class Connection(object):
    """ Class that represents a connection to an open database"""

    __slots__ = ['filename', 'database', 'transaction_mode', 'transaction_lock', 'lock_system']

    def __init__(self, filename):
        """
        Takes a filename, but doesn't do anything with it.
        (The filename will be used in a future project).
        :param filename: str name of the file.
        """
        self.filename = filename  # the name of the file
        self.database = None  # the database object we are connected to for this connection
        self.transaction_mode = None  # the transaction mode this connection is in
        self.transaction_lock = None  # the type of lock this connection has as a string
        self.lock_system = None  # the lock system for the database

        if filename not in _ALL_DATABASES.keys():
            # Create a database object inside the global database with key as filename
            try:
                _ALL_DATABASES[filename] = read_json_file(filename)
            except FileNotFoundError:
                _ALL_DATABASES[filename] = Database(filename)
            _ALL_DATABASES_LOCKSYSTEMS[filename] = LockSystem(filename)

        # Connect to the database object and lock system object with keys as filename inside global collections
        self.database = _ALL_DATABASES[filename]
        self.lock_system = _ALL_DATABASES_LOCKSYSTEMS[filename]

    def executemany(self, statement, params):
        """
        Takes a SQL statement and parameters to carry out a parameterized query.
        :param statement: a parameterized SQL statement.
        :param params: the list of tuple parameters to be slotted in the statement.
        """
        for param in params:
            param_statement = statement
            param_itr = 0
            while "?" in param_statement and param_itr != len(param):
                new_param = param[param_itr]
                new_param = "'" + new_param + "'" if isinstance(new_param, str) else str(new_param)
                param_statement = param_statement.replace("?", new_param, 1)
                param_itr += 1
            self.execute(param_statement)

        return None

    def execute(self, statement):
        """
        Takes a SQL statement.
        :param statement: a single SQL statement.
        :return: list of tuples (empty unless select statement with rows to return).
        """
        keywords = ["CREATE", "SELECT", "INSERT", "DELETE", "FROM", "ORDER", "GROUP", "WHERE"]
        aggregate_keywords = ["MAX", "MIN"]
        # Set statement_tokens to the result of tokenizing the statement
        statement_tokens = tokenize(statement)

        #  Check if valid sql statement ending in ';'
        if len(statement_tokens) == 0 or statement_tokens[-1] != ";":
            raise AssertionError("Statement is not a valid SQL statement!")

        #  BEGIN TRANSACTION Statements
        if statement_tokens[0] == "BEGIN" and statement_tokens[-2] == "TRANSACTION":
            if self.transaction_mode is not None:
                raise Exception("Beginning transaction attempted within an open transaction!")

            mode = statement_tokens[1]
            if mode == "DEFERRED" or mode == "TRANSACTION":
                self.database = copy.deepcopy(_ALL_DATABASES[self.filename])
                self.transaction_mode = "DEFERRED"
                self.transaction_lock = None
            elif mode == "IMMEDIATE":
                self.database = copy.deepcopy(_ALL_DATABASES[self.filename])
                self.transaction_mode = "IMMEDIATE"
                if self.lock_system.add_lock("reserved"):
                    self.transaction_lock = "reserved"
            elif mode == "EXCLUSIVE":
                self.database = copy.deepcopy(_ALL_DATABASES[self.filename])
                self.transaction_mode = "EXCLUSIVE"
                if self.lock_system.add_lock("exclusive"):
                    self.transaction_lock = "exclusive"
            else:
                raise Exception("Transaction mode attempted is one not supported!")

        #  COMMIT TRANSACTION Statements
        elif statement_tokens[0] == "COMMIT" and statement_tokens[1] == "TRANSACTION":
            if self.transaction_mode is None:
                raise Exception('Committing transaction attempted without a prior BEGIN TRANSACTION')
            else:
                #  Handle case if empty transaction
                if self.transaction_lock is None:
                    self.transaction_mode = None
                    self.database = _ALL_DATABASES[self.filename]

                if self.transaction_lock == "shared":
                    self.lock_system.remove_lock(self.transaction_lock)
                    self.transaction_lock = None
                    self.transaction_mode = None
                    self.database = _ALL_DATABASES[self.filename]

                elif self.transaction_lock == "reserved":
                    if self.lock_system.add_lock("exclusive", self.transaction_lock):
                        self.transaction_lock = "exclusive"
                        self.lock_system.remove_lock(self.transaction_lock)
                        self.transaction_lock = None
                        self.transaction_mode = None
                        _ALL_DATABASES[self.filename] = self.database

                elif self.transaction_lock == "exclusive":
                    self.lock_system.remove_lock(self.transaction_lock)
                    self.transaction_lock = None
                    self.transaction_mode = None
                    _ALL_DATABASES[self.filename] = self.database

            return None

        #  ROLLBACK TRANSACTION Statements
        elif statement_tokens[0] == "ROLLBACK" and statement_tokens[1] == "TRANSACTION":
            if self.transaction_mode is None:
                raise Exception('Rolling back transaction attempted outside of manual transaction')
            else:
                self.lock_system.remove_lock(self.transaction_lock)
                self.transaction_lock = None
                self.transaction_mode = None
                self.database = _ALL_DATABASES[self.filename]
            return None

        #  CREATE TABLE Statement
        elif statement_tokens[0] == "CREATE" and statement_tokens[1] == "TABLE":
            #
            # Check locks
            #
            self.lock_check('write')

            if statement_tokens[2] == "IF" and statement_tokens[3] == "NOT" and statement_tokens[4] == "EXISTS":
                table_name = statement_tokens[5]
                if table_name in self.database.tables:
                    return None
            else:
                table_name = statement_tokens[2]
                # Throw exception if trying to create a table that already exists
                if table_name in self.database.tables:
                    raise Exception('Trying to create a pre-existing table')
            schema_begin = statement_tokens.index(table_name) + 2
            schema_end = -2
            schema = statement_tokens[schema_begin:schema_end]
            self.database.add_table(table_name, schema)

            if self.transaction_mode is None:
                self.lock_check("commit")

            return None

        #  CREATE VIEW Statement
        elif statement_tokens[0] == "CREATE" and statement_tokens[1] == "VIEW":
            #
            # Check locks
            #
            self.lock_check('read')

            view_name = statement_tokens[2]
            # Throw exception if trying to create a view that already exists
            if view_name in self.database.tables:
                raise Exception('Trying to create a pre-existing view')

            from_index = statement_tokens.index("FROM")
            select_index = statement_tokens.index("SELECT")
            view_columns = [column_name for column_name in statement_tokens[select_index + 1:from_index] if
                            column_name != ","]
            table_name = statement_tokens[from_index + 1]
            view_statement = statement[statement.find("SELECT"):]

            # Check if viewing a joined table from a select statement that would not be in the database
            if statement_tokens[from_index + 2] == "LEFT" and statement_tokens[from_index + 3] == "OUTER" and statement_tokens[from_index + 4] == "JOIN":
                result = self.execute(view_statement)
                table_name = self.database.joined_table.name
                table_schema = self.database.joined_table.schema
            elif table_name not in self.database.tables:
                raise Exception('Trying to create view for a table that does not exist')
            # Normal case
            else:
                result = self.execute(view_statement)
                table_schema = self.database.tables[table_name].schema

            # Promote lock to exclusive to write view to the database
            self.lock_check("write")

            self.database.add_view(view_name, table_name, view_columns, table_schema, view_statement)
            view = self.database.tables[view_name]
            for row in result:
                row_list = list(row)
                view.insert_row(row_list, None)

            self.database.joined_table = None  # added
            if self.transaction_mode is None:
                self.lock_check("commit")
            return None

        #  DROP TABLE Statement
        elif statement_tokens[0] == "DROP" and statement_tokens[1] == "TABLE":
            #
            # Check locks
            #
            self.lock_check('write')

            if statement_tokens[2] == "IF" and statement_tokens[3] == "EXISTS":
                table_name = statement_tokens[4]
                if table_name not in self.database.tables:
                    return None
            else:
                table_name = statement_tokens[2]
                if table_name not in self.database.tables:
                    raise Exception("Trying to drop a table that is not in the database")
            self.database.remove_table(table_name)

            if self.transaction_mode is None:
                self.lock_check("commit")

            return None

        #  INSERT INTO Statement
        elif statement_tokens[0] == "INSERT" and statement_tokens[1] == "INTO":
            #
            #  Check locks
            #
            self.lock_check("write")

            table_name = statement_tokens[2]
            table = self.database.tables[table_name]
            values_index = statement_tokens.index("VALUES")
            columns_to_insert = None

            #  Check if just inserting default values
            if statement_tokens[values_index - 1] == "DEFAULT":
                table.insert_row("DEFAULT", columns_to_insert)

                if self.transaction_mode is None:
                    self.lock_check("commit")
                return None

            #  Check if inserting into certain columns
            if values_index - 3 != 0:
                cols_start = statement_tokens.index('(', 2) + 1
                cols_end = statement_tokens.index(')', cols_start, values_index)
                columns_to_insert = [column_name for column_name in statement_tokens[cols_start:cols_end] if column_name != ","]

            # Handle single as well as multiple insertions
            value_entry = None
            values_end = statement_tokens.index(';', values_index)
            entry_start = statement_tokens.index('(', values_index) + 1
            entry_end = statement_tokens.index(')', entry_start)
            while entry_start < values_end:
                value_entry = [value for value in statement_tokens[entry_start:entry_end] if value != "," and value != "("]
                table.insert_row(value_entry, columns_to_insert)
                if "(" not in statement_tokens[entry_end:]:
                    break
                entry_start = statement_tokens.index('(', entry_end)
                entry_end = statement_tokens.index(')', entry_start)
                value_entry = [value for value in statement_tokens[entry_start:entry_end] if value != ","]

            if self.transaction_mode is None:
                self.lock_check("commit")

            return None

        #  DELETE Statement
        elif statement_tokens[0] == "DELETE" and statement_tokens[1] == "FROM":
            #
            # Check Locks
            #
            self.lock_check("write")

            table_name = statement_tokens[2]
            conditions = None
            if "WHERE" in statement_tokens[1:]:
                where_index = statement_tokens.index("WHERE")
                conditions = [value for value in statement_tokens[where_index + 1:-1] if value is None or value != ","]
            self.delete_from(table_name, conditions)

            if self.transaction_mode is None:
                self.lock_check("commit")

            return None

        #  UPDATE Statement
        elif statement_tokens[0] == "UPDATE":
            #
            # Check Locks
            #
            self.lock_check("write")

            table_name = statement_tokens[1]
            table = self.database.tables[table_name]
            conditions = None
            set_values = None
            end_index = statement_tokens.index(";")

            if "WHERE" in statement_tokens[1:end_index]:
                where_index = statement_tokens.index("WHERE")
                end_index = where_index
                conditions = [value for value in statement_tokens[where_index + 1:-1] if value is None or value != ","]

            if "SET" in statement_tokens[1:end_index]:
                set_index = statement_tokens.index("SET")
                set_values = [value for value in statement_tokens[set_index + 1:end_index] if value is None or value != "," and value != "="]
                set_pairs = []
                for index in range(0, len(set_values), 2):
                    col = set_values[index]
                    val = set_values[index + 1]
                    set_pairs.append([col, val])
                set_values = set_pairs

            self.update_table(table, set_values, conditions)
            if self.transaction_mode is None:
                self.lock_check("commit")

            return None

        #  LEFT OUTER JOIN Statement
        elif "LEFT" in statement_tokens and "OUTER" in statement_tokens and "JOIN" in statement_tokens:
            #
            # Check locks
            #
            self.lock_check("read")

            distinct_columns = None
            conditions = None
            order = None
            join_key_start = statement_tokens.index("LEFT")
            join_key_end = statement_tokens.index("JOIN", join_key_start)

            #  Tables
            table1_name = statement_tokens[join_key_start - 1]
            table2_name = statement_tokens[join_key_end + 1]
            table1 = self.database.tables[table1_name]
            table2 = self.database.tables[table2_name]

            #  Gather the predicate to join on between tables
            on_index = statement_tokens.index("ON", join_key_end)
            on_conditions = [value for value in statement_tokens[on_index + 1:on_index + 4]
                             if value is None or value != "," and value not in keywords]

            # Check Order
            if "ORDER" in statement_tokens[1:]:
                order_index = statement_tokens.index("ORDER", on_index, statement_tokens.index(";"))
                order = [column_name for column_name in statement_tokens[order_index + 2:statement_tokens.index(";")] if column_name != ","]

            #  Gather the columns to display
            columns = []
            for column in statement_tokens[1:join_key_start - 2]:
                if table1_name in column or table2_name in column:
                    columns.append(column)
                elif column == ",":
                    continue
                elif "*" in column:
                    columns.append(column)
                elif column in table1.columns:
                    columns.append(table1_name + "." + column)

            #  Create one large table with entries from both tables
            joined_table = Table("JoinedTable")
            for col_name in table1.schema:
                col_type = table1.schema[col_name]
                joined_table.add_column(table1.name + "." + col_name, col_type)
            for col_name in table2.schema:
                col_type = table2.schema[col_name]
                joined_table.add_column(table2.name + "." + col_name, col_type)

            #  Creates list of table1 primary_keys and table2 rows that match the key comparison
            primary_keys = []
            foreign_rows = []
            table1_comparator = table1.get_column_index(on_conditions[0][len(table1_name) + 1:])
            table2_comparator = table2.get_column_index(on_conditions[-1][len(table2_name) + 1:])
            for row in table1.rows:
                primary_keys.append(row.data[table1_comparator])
            for row in table2.rows:
                if row.data[table2_comparator] in primary_keys:
                    foreign_rows.append(row)

            #  Add matching table rows together into the joined table
            for row in table1.rows:
                primary_key = row.data[table1_comparator]
                match_found = False
                for foreign_row in foreign_rows:
                    #  If match found, join rows and add to table
                    if primary_key == foreign_row.data[table2_comparator]:
                        joined_row = list(row.data) + list(foreign_row.data)
                        joined_table.insert_row(joined_row, None)
                        match_found = True
                        break
                #  No matching key, fill in with None
                if not match_found:
                    offset = [None] * table2.column_size
                    joined_row = list(row.data) + offset
                    joined_table.insert_row(joined_row, None)

            # Add joined table to the database as most recently joined table
            self.database.joined_table = joined_table
            query_result = self.select(joined_table, columns, distinct_columns, conditions, order)
            # Release locks if auto-commit
            if self.transaction_mode is None:
                self.lock_check("relinquish")
            #  If nothing returned from query then return an empty list
            return query_result if len(query_result) > 0 else []

        #  SELECT Statement
        elif statement_tokens[0] == "SELECT":
            #
            # Lock Checks
            #
            self.lock_check("read")

            distinct_columns = None
            from_keyword = statement_tokens.index('FROM')
            curr_index = 1
            table_name = statement_tokens[from_keyword + 1]
            table = self.database.tables[table_name]

            if isinstance(table, View):
                table_schema = copy.deepcopy(self.database.tables[table_name].schema)
                view_statement = copy.deepcopy(table.statement)
                view_columns = copy.deepcopy(table.view_columns)
                view_name = copy.deepcopy(table.name)

                result = self.execute(view_statement)

                new_view = View(view_name, table_name, view_columns, table_schema, view_statement)
                for row in result:
                    row_list = list(row)
                    new_view.insert_row(row_list, None)
                table = new_view
                self.database.tables[table.name] = table

            # Check if using an aggregate function
            aggregate = None
            aggregate_index = None
            for keyword in aggregate_keywords:
                if keyword in statement_tokens:
                    aggregate_index = statement_tokens.index(keyword)
                    if aggregate_index < from_keyword:
                        aggregate = keyword
                        break

            if "DISTINCT" in statement_tokens[:from_keyword]:
                distinct_columns = []
                distinct_index = statement_tokens.index('DISTINCT')
                distinct_columns.append(statement_tokens[distinct_index + 1])
                curr_index = distinct_index

            # Check if qualified
            columns = []
            for column in statement_tokens[curr_index:from_keyword]:
                if "." in column and table_name in column:
                    columns.append(column[len(table_name) + 1:])
                elif column != "," and column != "DISTINCT":
                    columns.append(column)

            order = None
            conditions = None
            curr_index = from_keyword
            #  Check conditionals
            if "WHERE" in statement_tokens[from_keyword:]:
                where_index = statement_tokens.index("WHERE")
                conditions = [value for value in statement_tokens[where_index + 1:where_index + 4]
                              if value is None or value != "," and value not in keywords]
                curr_index += len(conditions) + 1
                if "." in conditions[0] and table_name in conditions[0]:
                    conditions[0] = conditions[0][len(table_name) + 1:]

            #  Check ordering
            if "ORDER" in statement_tokens[curr_index + 1:]:
                qual_column = []
                order_index = statement_tokens[curr_index + 4:]
                order = [column_name for column_name in order_index[0:-1] if column_name != ","]
                # Check if qualified
                for column in order:
                    if "." in column and table_name in column:
                        qual_column.append(column[len(table_name) + 1:])
                    else:
                        qual_column.append(column)
                order = qual_column

            # Call to select function to handle rest of query
            query_result = self.select(table, columns, distinct_columns, conditions, order)
            # Release locks for auto commit mode
            if self.transaction_mode is None:
                self.lock_check("relinquish")

            #  If using aggregate function return that result
            if aggregate:
                if aggregate == "MAX":
                    return [max(query_result)]
                elif aggregate == "MIN":
                    return [min(query_result)]

            #  If nothing returned from query then return an empty list
            return query_result if len(query_result) > 0 else []

        return None

    def select(self, table: Table, columns: List[str], distinct: List[str] | None, conditions: List[str] | None, order):
        """
        Execute a SQL SELECT statement
        :param table: the table to run the select statement on.
        :param columns: list of columns that should be returned.
        :param distinct: list of distinct columns or None.
        :param conditions: the list of conditions or None.
        :param order: list of ordering the return should follow, None if no ordering.
        :return: list of column values for rows in a table matching the query.
        """
        result = []
        op = None
        value = None
        distinct_column_set = set()

        if conditions:
            #  Check case if comparing to Null value
            op = conditions[1]
            if conditions[1] == "IS":
                if conditions[2] and conditions[2] == "NOT":
                    op += " " + conditions[2]
            else:
                value = conditions[2]

        #  Handle case if selecting certain columns
        if len(columns) > 0:
            for row in table.rows:
                if conditions is None:
                    record = row.data
                    result.append(record)
                else:
                    column_name = conditions[0]
                    column_index = table.column_names.index(column_name)
                    if row.check_row(column_name, column_index, op, value):
                        result.append(row.data)

            # Handle case if there is ordering
            if order:
                order_indices = [table.get_column_index(name) for name in order if name in table.column_names]
                rev = True if "DESC" in order else False
                if len(order_indices) == 1:
                    result.sort(key=itemgetter(order_indices[0]), reverse=rev)
                else:
                    result.sort(key=itemgetter(order_indices[0], order_indices[1]), reverse=rev)

            slimmed_result = []
            columns_index = []

            #  Collect the indices of the columns we want to view
            for name in columns:
                if name in table.column_names:
                    columns_index.append(table.column_names.index(name))
                elif name in "*":
                    columns_index.append("*")

            #  Slim down the results to only rows that meet conditions
            for record in result:
                slimmed_record = []
                for index in columns_index:
                    if index != "*":
                        if distinct is not None and table.column_names[index] == distinct[0]:
                            if record[index] not in distinct_column_set:
                                slimmed_record.append(record[index])
                                distinct_column_set.add(record[index])
                        else:
                            slimmed_record.append(record[index])
                    else:
                        if distinct is not None and index == distinct[0]:
                            if record not in distinct_column_set:
                                slimmed_record.append(record)
                                distinct_column_set.add(record)
                        else:
                            slimmed_record.extend(record)
                #  Check to make sure the entry has content
                if len(slimmed_record) != 0:
                    slimmed_result.append(tuple(slimmed_record))
            result = slimmed_result

        return result

    def update_table(self, table: Table, values: List[T] | None, conditions: List[T] | None) -> None:
        """
        Update a table in the database
        :param table: the table object to update in teh database
        :param values: the values to set.
        :param conditions: the conditions to be met for the values to update, None if unconditional
        """
        #  Check if table is empty
        if table.size == 0:
            return None

        #  If unconditional update, assign rows new values
        if conditions is None:
            for row in table.rows:
                for value in values:
                    index = table.get_column_index(value[0])
                    row.update_row(index, value[1])
            return None

        column_name = conditions[0]
        column_index = table.get_column_index(column_name)
        op = conditions[1]
        val = None
        #  Check case if comparing to Null value
        if conditions[1] == "IS":
            if conditions[2] and conditions[2] == "NOT":
                op += " " + conditions[2]
        else:
            val = conditions[2]

        #  Update corresponding row entries
        rows_to_update = []
        for row in table.rows:
            # Check condition for update
            if row.check_row(column_name, column_index, op, val):
                rows_to_update.append(row)

        for row in rows_to_update:
            for value in values:
                index = table.get_column_index(value[0])
                row.update_row(index, value[1])

        return None

    def delete_from(self, table_name: str, conditions: List[T] | None) -> None:
        """
        Remove all rows from a table or those that match a predicate
        :param table_name: the name of the table to delete rows from
        :param conditions: the list of conditions or None
        """
        table = self.database.tables[table_name]

        #  Check if table is already empty
        if table.size == 0:
            return None

        #  Delete all rows if unconditional delete from
        if conditions is None:
            table.rows = []
            table.size = 0
            return None

        column_name = conditions[0]
        column_index = table.get_column_index(column_name)
        op = conditions[1]
        value = None
        #  Check case if comparing to Null value
        if conditions[1] == "IS":
            if conditions[2] and conditions[2] == "NOT":
                op += " " + conditions[2]
        else:
            value = conditions[2]

        #  Collect the rows to remove
        rows_to_remove = []
        for row in table.rows:
            if row.check_row(column_name, column_index, op, value):
                rows_to_remove.append(row)
        #  Remove the collected rows from the table
        for row in rows_to_remove:
            table.remove_row(row)

        return None

    def lock_check(self, action) -> None:
        """
        Check locks on the connected database.
        :param action: the str name of the action requiring a lock.
        """
        if self.transaction_mode is None:
            if action == "read":
                if self.lock_system.add_lock("shared", self.transaction_lock):
                    self.transaction_lock = "shared"
                    self.database = _ALL_DATABASES[self.filename]
            elif action == "write":
                if self.lock_system.add_lock("exclusive", self.transaction_lock):
                    self.transaction_lock = "exclusive"
                    self.database = copy.deepcopy(_ALL_DATABASES[self.filename])
            elif action == "commit":
                if self.lock_system.add_lock("exclusive", self.transaction_lock):
                    self.transaction_lock = "exclusive"
                    self.lock_system.remove_lock(self.transaction_lock)
                    self.transaction_lock = None
                    _ALL_DATABASES[self.filename] = self.database
            elif action == "relinquish":
                self.lock_system.remove_lock(self.transaction_lock)
                self.transaction_lock = None
            return None

        elif self.transaction_mode == "DEFERRED":
            if action == "read":
                # Check if already have a lock
                if self.transaction_lock:
                    return None
                if self.lock_system.add_lock("shared", self.transaction_lock):
                    self.transaction_lock = "shared"
                    return None
            elif action == "write":
                if self.lock_system.add_lock("reserved", self.transaction_lock):
                    self.transaction_lock = "reserved"
                    return None
    
        elif self.transaction_mode == "IMMEDIATE":
            if action == "read":
                return None
            if action == "write":
                if self.transaction_lock and self.transaction_lock == "exclusive":
                    return None
                elif self.lock_system.add_lock("reserved", self.transaction_lock):
                    self.transaction_lock = "reserved"
                    return None

        elif self.transaction_mode == "EXCLUSIVE":
            if action == "read" or action == "write":
                return None

        return None

    def close(self):
        """
        Write the database to disk and end the connection.
        """
        write_json_file(self.filename, self.database)
        return None


def connect(filename, timeout=0.1, isolation_level=None):
    """
    Creates a Connection object with the given filename.
    :param filename: str name of the given file.
    :param timeout: max runtime until timout
    :param isolation_level:
    :return: a connection object with the given file.
    """
    return Connection(filename)


class Database(object):
    """ Class representing a collection of Table objects"""

    __slots__ = ['name', 'size', 'tables', 'joined_table']

    def __init__(self, database_name: str = "") -> None:
        """
        Initializes a Database object.
        :param database_name: the str name of the Database.
        """
        self.name = database_name  # the name for the database
        self.size = 0  # the number of tables in the database
        self.tables = {}  # the dictionary of tables { table key : table object }
        self.joined_table = None  # the most recent joined table to be used or discarded after a transaction

    def __eq__(self, other: Database) -> bool:
        """
        Overloads the equality operator for Database class.
        :param other: the Database to compare against self.
        :return: True if equal, False otherwise.
        """
        # Check attributes
        if self.size != other.size or self.name != other.name:
            return False
        # Check each tables
        for table, other_table in zip(self.tables.values(), other.tables.values()):
            if table != other_table:
                return False
        # Equality passed
        return True

    def add_table(self, table_name: str, values: list = []) -> None:
        """
        Add a Table object and add it to the database.
        :param table_name: the str name for the Table.
        :param values: the list of column names with their data type.
        """
        table = Table(table_name, values)
        self.tables[table_name] = table
        self.size += 1

    def remove_table(self, table_name: str) -> None:
        """
        Remove a Table object from the database.
        :param table_name: the str name for the Table.
        """
        self.tables.pop(table_name)
        self.size -= 1
        return None

    def add_view(self, view_name: str, table_name: str, view_columns: List[T], schema: Dict, statement: str) -> None:
        """
        Add a View object to the database.
        :param view_name: the str name for the View.
        :param table_name: the str name for the Table.
        :param view_columns: the columns in the table that will be viewed.
        :param schema: the schema this view will verify with its columns.
        :param statement: the statement the view will run to initialize.
        """
        view = View(view_name, table_name, view_columns, schema, statement)
        self.tables[view_name] = view
        self.size += 1


class LockSystem(object):
    """ Class representing a locking system for a database"""

    __slots__ = ['name', 'shared', 'reserved', 'exclusive']

    def __init__(self, database_name: str = "") -> None:
        """
        Initializes a LockSystem object.
        """
        self.name = database_name  # the str name of the database this LockSystem is monitoring
        self.shared = 0  # the number of connection objects that have a shared lock on the database
        self.reserved = 0  # the connection object that has a reserved lock on the database
        self.exclusive = 0  # the connection object that has an exclusive lock on the database

    def __repr__(self):
        """
        Get a representation of the LockSystem class for debugging.
        :return result: str representation of current locks in the class.
        """
        result = "Lock System for database {name}\n".format(name=self.name)
        result += "Shared Locks: {lock}\n".format(lock=self.shared)
        result += "Reserved Locks: {lock}\n".format(lock=self.reserved)
        result += "Exclusive Locks: {lock}\n".format(lock=self.exclusive)
        return result

    def has_shared(self) -> bool:
        """
        Check if there are connection objects with shared locks.
        :return: True if there are reserved locks, False  otherwise.
        """
        return self.shared != 0

    def shared_count(self):
        """
        Get the count of shared locks on the database
        :return: int number of shared locks
        """
        return self.shared

    def has_reserved(self) -> bool:
        """
        Check if there is a reserved lock.
        :return: True if there is a reserved lock, False otherwise.
        """
        return self.reserved != 0

    def has_exclusive(self) -> bool:
        """
        Check if there is an exclusive lock.
        :return: True if there is a reserved lock, False otherwise.
        """
        return self.exclusive != 0

    def add_shared(self):
        """
        Add a shared lock for a connection.
        :return: True if a shared lock is added.
        """
        if not self.has_exclusive():
            self.shared += 1
            return True
        else:
            raise Exception("Trying to gain a shared lock while another connection has exclusive!")
            return False

    def remove_shared(self):
        """
        Remove a shared lock.
        """
        self.shared -= 1

    def add_reserved(self, current_lock=None):
        """
        Add a reserved lock for a connection.
        :return: True if a reserved lock is added.
        """
        if self.has_reserved():
            raise Exception("Trying to gain a reserved lock while another reserved lock is claimed!")
            return False
        elif self.has_exclusive():
            raise Exception("Trying to gain a reserved lock while an exclusive lock is claimed!")
            return False

        else:
            if current_lock and current_lock == "shared":
                self.remove_shared()
            self.reserved += 1
            return True

    def remove_reserved(self) -> None:
        """
        Remove a reserved lock.
        """
        self.reserved -= 1
        return None

    def add_exclusive(self, current_lock=None):
        """
        Add an exclusive lock for a connection.
        :param current_lock: the str name of the type of lock currently held or None
        :return: True if an exclusive lock is added.
        """
        if self.has_exclusive():
            raise Exception("Trying to gain an exclusive lock while another exclusive lock is claimed!")
            return False

        if self.has_shared():
            raise Exception("Trying to gain an exclusive lock while there are shared locks claimed.")
            return False

        if self.has_reserved():
            if current_lock and current_lock == "reserved":
                self.reserved -= 1
                self.exclusive += 1
                return True
            else:
                raise Exception("Trying to gain an exclusive lock while another reserved lock is claimed!")
                return False
        else:
            self.exclusive += 1
            return True

    def remove_exclusive(self) -> None:
        """
        Remove an exclusive lock.
        """
        self.exclusive -= 1
        return None

    def add_lock(self, lock_type, current_lock=None):
        """
        Add a lock for a connection.
        :param lock_type: the str name of the type of lock requested.
        :param current_lock: the str name of the type of lock currently held or None.
        :return: True if a lock is added or already the type held by the connection.
        """
        #  Check if the current lock held is already the type requested
        if current_lock and current_lock == lock_type:
            return True

        if lock_type == "shared":
            return self.add_shared()
        elif lock_type == "reserved":
            return self.add_reserved(current_lock)
        elif lock_type == "exclusive":
            return self.add_exclusive(current_lock)

    def remove_lock(self, lock_type):
        """
        Remove a lock.
        :param lock_type: the str name of the type of lock to relinquish.
        """
        if lock_type is None:
            return None
        elif lock_type == "shared":
            self.remove_shared()
        elif lock_type == "reserved":
            self.remove_reserved()
        elif lock_type == "exclusive":
            self.remove_exclusive()


class Table(object):
    """ Class representing a relation object inside a database"""

    __slots__ = ['name', 'schema', 'column_names', 'default_values', 'column_size', 'rows', 'size']

    def __init__(self, table_name: str = "", values: list = []) -> None:
        """
        Initializes a Table object.
        :param table_name: the str name of the table.
        :param values: schema values
        """
        self.name = table_name  # the name for the table
        self.schema = {}  # the schema for the table
        self.column_names = []  # the list of column names for the table
        self.default_values = {}  # the dictionary of default values for columns
        self.column_size = 0  # the number of columns for the table
        self.rows = []  # list of tuples containing the data of each row
        self.size = 0  # the number of rows for the table

        value_itr = 0
        # Iterate through list of schema values
        while len(values) != 0 and value_itr != len(values) - 1:
            self.column_size += 1
            column_name = values[value_itr]
            value_itr += 1
            column_type = values[value_itr]
            self.schema[column_name] = column_type
            value_itr += 1
            if value_itr == len(values):
                break
            value_itr += 1

        # Get default values
        value_itr = 0
        while "DEFAULT" in values[value_itr:]:
            default_index = values.index("DEFAULT")
            column_name = values[default_index - 2]
            default_value = values[default_index + 1]
            self.default_values[column_name] = default_value
            value_itr = default_index + 1

        self.column_names = list(self.schema)
        self.column_size = len(self.column_names)

    def __eq__(self, other: Table) -> bool:
        """
        Overloads the equality operator for Table class.
        :param other: the Table to compare against self.
        :return: True if equal, False otherwise.
        """
        # Check for dimensions
        if self.size != other.size or self.column_size != other.column_size:
            return False
        # Check for other attributes
        if self.name != other.name or self.schema != other.schema or self.column_names != other.column_names:
            return False
        # Check for each column
        for row, other_row in zip(self.rows, other.rows):
            if row != other_row:
                return False
        # Equality passed
        return True

    def add_column(self, column_name: str, column_type: str | None) -> None:
        """
        Add a column to a table
        :param column_name: the name of the column to add
        :param column_type: the type of the column to add
        """
        self.schema[column_name] = column_type
        self.column_names.append(column_name)
        self.column_size += 1

    def get_column_index(self, column_name: T):
        """
        Return the index of the column in a table
        :param column_name: the name of the column in a table.
        """
        return self.column_names.index(column_name)

    def insert_row(self, values: List[T] | str, insertion_columns: List[str] | None) -> None:
        """
        Insert a row to the table.
        :param values: the list of values tied to a column in a row.
        :param insertion_columns: the list of specific columns to insert into, None if inserting into all
        """
        # Check if using only default values
        if isinstance(values, str):
            default_vals = []
            for name in self.column_names:
                if name in self.default_values:
                    default_vals.append(self.default_values[name])
                else:
                    default_vals.append(None)
            # Initialize and add a new Row of default values, return early
            row = Row(default_vals)
            self.rows.append(row)
            self.size += 1
            return None

        #  Check for input of more values than columns and ignore insertion request if True
        if len(values) > self.column_size:
            return None

        #  Checks cases associated with inserting specific columns
        if insertion_columns is not None and values is not None:
            #  Fill difference in input value size with None values
            while len(insertion_columns) - len(values) > 0:
                values.append(None)

            # Start reorganization of input row data
            cols_to_values = {}
            for col, vals in zip(insertion_columns, values):
                cols_to_values[col] = vals
            values = []
            # Match each col val pair input to the correct index for an entry
            for column_name in self.column_names:
                if column_name in insertion_columns:
                    values.append(cols_to_values[column_name])
                #  Check if there is a default value
                elif column_name in self.default_values:
                    values.append((self.default_values[column_name]))
                else:
                    values.append(None)

        #  Check cases if inserting fewer values than there are columns
        elif values is not None and len(values) < self.column_size:

            while len(values) - self.column_size > 0:
                values.append(None)

        #  Check if value input matches schema, if not then ignore insertion request
        for value, value_type in zip(values, self.schema.values()):
            if value_type is None and value is None or value is None:
                continue
            if value_type == "TEXT" and isinstance(value, str):
                continue
            elif value_type == "REAL" and isinstance(value, float):
                continue
            elif value_type == "INTEGER" and isinstance(value, int):
                continue
            elif value_type == "BLOB":
                continue
            return None

        # Initialize and add a new Row
        row = Row(values)
        self.rows.append(row)
        self.size += 1
        return None

    def remove_row(self, row: Row) -> None:
        """
        Remove a row from the table.
        :param row: the Row object to remove
        """
        if self.size == 0:
            return None

        # Remove a matching record
        for record in self.rows:
            if row == record:
                self.rows.remove(record)
                self.size -= 1
                return None

        return None


class View(Table):
    """ Class representing a table viewing object in a database"""

    def __init__(self, view_name: str = "", table_name: str = "", view_columns: List[T] = [], table_schema: Dict = {}, statement: str = ""):
        """
        Initializes a View object.
        :param view_name: the str name of the view.
        :param table_name: the str name of the table this object is viewing.
        :param view_columns: the columns from the table to view.
        :param table_schema: the schema of the table this object is viewing.
        :param statement: the statement used to initialize on.
        """
        self.statement = statement  # the statement initializing the object values.
        self.view_columns = view_columns  # the columns to be viewed.
        self.table_name = table_name  # the name of the table being viewed

        view_schema = {}
        unqualified_schema = {}
        for table_cols in table_schema:
            if "." in table_cols:
                unqualified_col = table_cols[table_cols.find(".") + 1:]
                unqualified_schema[unqualified_col] = table_schema[table_cols]
            else:
                unqualified_schema[table_cols] = table_schema[table_cols]

        if "*" in view_columns and len(view_columns) == 1:
            view_schema = unqualified_schema
        else:
            for column in view_columns:
                if column in unqualified_schema:
                    view_schema[column] = unqualified_schema[column]

        super().__init__(view_name, [])
        self.schema = view_schema
        self.column_names = list(self.schema)
        self.column_size = len(self.column_names)

    def update_view(self, view_result, table_name: str = "", table_schema: Dict = {}):
        """
        Update the data inside the View object to reflect global changes.
        :param view_result: the result of running the original statement again.
        :param table_name: the str name of the table this object is viewing.
        :param table_schema: the schema of the table this object is viewing.
        """
        self.schema = table_schema
        self.column_names = list(self.schema)
        self.column_size = len(self.column_names)
        self.size = 0
        self.rows = []


class Row(object):
    """ Class representing a record object inside a table"""

    __slots__ = ['data', 'size', 'primary_key']

    def __init__(self, row_data: list = []) -> None:
        """
        Initializes a Row object.
        :param row_data: A list of data to put into this row.
        """
        self.data = tuple(row_data)
        self.size = len(row_data)
        self.primary_key = None

    def __eq__(self, other: Row) -> bool:
        """
        Overloads the equality operator for Row class.
        :param other: the Row to compare against self.
        :return: True if equal, False otherwise.
        """
        # Check for same length and data content
        if self.size != other.size or self.data != other.data:
            return False
        # Equality passed
        return True

    def set_primary_key(self, key: T) -> None:
        """
        Set the primary key for a row
        """
        self.primary_key = self.data[key]

    def update_row(self, column_index: int, value: T) -> None:
        """
        Update the values of a row
        :param column_index: the index of the column to update
        :param value: the new value to set for an entry
        """
        row_data = list(self.data)
        row_data[column_index] = value
        self.data = tuple(row_data)
        return None

    def check_row(self, column_name: str, column_index: int, op: str, value: T) -> bool:
        """
        Check a row to see if it passes a predicate
        :param column_name: the name of the column to check
        :param column_index: the index of the column to check
        :param op: the comparison operator to compare with
        :param value: the value to compare against
        :return: True if the row passed the predicate, False otherwise.
        """
        column_value = self.data[column_index]

        # Check cases if value of the column is None
        if value is None:
            if op == "IS":
                return True if not column_value else False
            elif op == "IS NOT":
                return True if column_value else False
        elif column_value is None:
            return False

        #  Check normal operator cases
        if op == "=":
            return column_value == value
        elif op == "!=":
            return column_value != value
        elif op == ">":
            return column_value > value
        elif op == ">=":
            return column_value >= value
        elif op == "<":
            return column_value < value
        elif op == "<=":
            return column_value <= value

        #  Return False if operator did not match expected
        return False


def collect_characters(query, allowed_characters):
    """
    Loop through query string until we hit something we are not looking for.
    :param query: the string we are collecting relevant characters for.
    :param allowed_characters: the string of allowed characters.
    :return: Return characters that are allowed.
    """
    letters = []
    for letter in query:
        if letter not in allowed_characters:
            break
        letters.append(letter)
    return "".join(letters)


def remove_leading_whitespace(query, tokens):
    """
    Splice a query string to remove leading whitespace and add token to token list.
    :param query: the string we are removing leading whitespace from.
    :param tokens: the list of our tokens.
    :return: splice of query beginning at the end of the leading whitespace.
    """
    whitespace = collect_characters(query, string.whitespace)
    return query[len(whitespace):]


def remove_word(query, tokens):
    """
    Splice a query string to shorten and add token to token list.
    :param query: the string of characters we want to add to our token list.
    :param tokens: the list of our tokens.
    :return: splice of query beginning at the end of the word.
    """
    word = collect_characters(query,
                              string.ascii_letters + "_.*" + string.digits)
    if word == "NULL":
        tokens.append(None)
    else:
        tokens.append(word)
    return query[len(word):]


def remove_text(query, tokens):
    """
    Remove text from a query and add to token to our token list.
    :param query: the string of characters we want to add to our token list.
    :param tokens: the list of our tokens.
    :return: splice of query beginning at the end of the text.
    """
    assert query[0] == "'"
    query = query[1:]
    end_quote_index = query.find("'")

    #  Escape double quotes
    if "''" in query:
        escape_index = query.find("''")
        while escape_index == end_quote_index and escape_index != -1:
            end_quote_index = query.find("'", escape_index + 2)
            escape_index = query.find("''", escape_index + 2)

    text = query[:end_quote_index]
    text = text.replace("''", "'")
    tokens.append(text)
    query = query[end_quote_index + 1:]
    return query


def remove_num(query, tokens):
    """
    Remove a number from the query and add token to our token list.
    :param query: the string of characters we want to add to our token list.
    :param tokens: the list of our tokens.
    :return: splice of query beginning at the end of the number.
    """
    number = collect_characters(query, string.digits + "-.E")
    if "." in number:
        tokens.append(float(number))
    else:
        tokens.append(int(number))
    return query[len(number):]


def tokenize(query) -> List:
    """
    Author: James Mariani
    Tokenize a query and return a list of tokens.
    :param query: the string that consists of a SQL query command.
    :return: list of string tokens from the query.
    """
    tokens = []
    while query:
        old_query = query

        if query[0] in string.whitespace:
            query = remove_leading_whitespace(query, tokens)
            continue
        if query[0] in (string.ascii_letters + "_"):
            query = remove_word(query, tokens)
            continue
        if query[0] in "(),;*":
            tokens.append(query[0])
            query = query[1:]
            continue
        if query[0] == "'":
            query = remove_text(query, tokens)
            continue

        if query[0].isnumeric() or query[0] in "-":
            query = remove_num(query, tokens)
            continue

        if query[0] in "<>=!":
            last_token = tokens[-1]
            if len(last_token) < 2:
                if last_token in "!>" and query[0] == "=":
                    tokens[-1] += query[0]
                    query = query[1:]
                    continue
                elif last_token == "=" and query[0] == "<":
                    tokens[-1] += query[0]
                    query = query[1:]
                    continue
            tokens.append(query[0])
            query = query[1:]

        if len(query) == len(old_query):
            raise AssertionError("Query didn't get shorter.")

    return tokens


def read_json_file(filename: str):
    """
    Takes a JSON formatted file and returns a data object.
    Reads JSON files.
    :param filename: The filename denoting a Json formatted file.
    :return data: Database object containing tables and other class information.
    """

    data = None
    db_name = None
    db_tables = {}
    db_joined_tables = None

    with open(filename, 'r', newline='', encoding="utf-8") as jsonfile:
        # Convert to python list which holds a dictionary object for each entry
        json_list = json.load(jsonfile)

        # Get the database attributes
        db_name = json_list["name"]
        db_size = json_list["size"]
        db_joined_table_name = json_list["joined_table"]

        # Get attributes for tables
        for table in json_list["tables"]:
            table_name = table["name"]
            table_rows = []

            loaded_table = Table(table_name)
            loaded_table.schema = table["schema"][0]
            loaded_table.column_names = list(loaded_table.schema)
            loaded_table.default_values = table["default_values"]
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
    if db_joined_table_name is None:
        data.joined_table = None
    else:
        #data.joined_table = data.tables[db_joined_table_name]
        data.joined_table = db_joined_table_name

    return data


def write_rows(row, column_names):
    """
    Create a kv pair for a row's column name and column entry
    :param row: Row object to write from.
    :param column_names: the names of the columns to pair data to.
    :return row_schema: the dict schema for a row.
    """
    row_schema = dict(zip(column_names, list(row.data)))
    return row_schema


def write_json_file(filename: str, data: Database):
    """
    Takes a filename(to be written to) and a data object.
    Writes JSON files.
    :param filename: The filename to be written to.
    :param data: Database object containing tables.
    """

    db_tables = data.tables

    # Create dictionary of attributes for database
    joined_tables = None if data.joined_table is None else data.joined_table.name
    json_dict = {"name": data.name, "joined_table": joined_tables, "size": data.size, "tables": []}

    for name, table in db_tables.items():
        # Create dictionary of attributes for table
        table_dict = {"name": table.name, "default_values": table.default_values, "schema": [table.schema]}
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
