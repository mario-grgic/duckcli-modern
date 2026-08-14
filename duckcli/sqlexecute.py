import logging
import duckdb
import uuid
from contextlib import closing

import sqlparse
import os.path
import json
import re

from .packages import special

_logger = logging.getLogger(__name__)

# FIELD_TYPES = decoders.copy()
# FIELD_TYPES.update({
#     FIELD_TYPE.NULL: type(None)
# })


class SQLExecute(object):

    databases_query = """
        PRAGMA database_list
    """

    tables_query = """
        SELECT table_name FROM information_schema.tables;
    """

    table_columns_query = """
        SELECT table_name, column_name FROM information_schema.columns;
    """

    functions_query = """
        SELECT DISTINCT function_name 
        FROM duckdb_functions();
    """

    def __init__(self, database):
        self.dbname = database
        self._server_type = None
        self.connection_id = None
        self.conn = None
        if not database:
            _logger.debug("Database is not specified. Skip connection.")
            return
        self.connect()

    def connect(self, database=None):
        db = database or self.dbname
        _logger.debug("Connection DB Params: \n" "\tdatabase: %r", database)

        db_name = os.path.expanduser(db)
        db_dir_name = os.path.dirname(os.path.abspath(db_name))
        if not os.path.exists(db_dir_name):
            raise Exception("Path does not exist: {}".format(db_dir_name))

        conn = duckdb.connect(database=db_name)
        if self.conn:
            self.conn.close()

        self.conn = conn
        # Update them after the connection is made to ensure that it was a
        # successful connection.
        self.dbname = db
        # retrieve connection id
        self.reset_connection_id()

    def run(self, statement):
        """Execute the sql in the database and return the results. The results
        are a list of tuples. Each tuple has 4 values
        (title, rows, headers, status).
        """
        # Remove spaces and EOL
        statement = statement.strip()
        if not statement:  # Empty string
            yield (None, None, None, None)

        # Split the sql into separate queries and run each one.
        # Unless it's saving a favorite query, in which case we
        # want to save them all together.
        if statement.startswith("\\fs"):
            components = [statement]
        else:
            components = sqlparse.split(statement)

        for sql in components:
            # Remove spaces, eol and semi-colons.
            sql = sql.rstrip(";")

            # \G is treated specially since we have to set the expanded output.
            if sql.endswith("\\G"):
                special.set_expanded_output(True)
                sql = sql[:-2].strip()

            if not self.conn and not (
                sql.startswith(".open")
                or sql.lower().startswith("use")
                or sql.startswith("\\u")
                or sql.startswith("\\?")
                or sql.startswith("\\q")
                or sql.startswith("help")
                or sql.startswith("exit")
                or sql.startswith("quit")
            ):
                _logger.debug(
                    "Not connected to database. Will not run statement: %s.", sql
                )
                raise Exception("Not connected to database.")
                # yield ('Not connected to database', None, None, None)
                # return

            cur = self.conn.cursor() if self.conn else None
            try:  # Special command
                _logger.debug("Trying a dbspecial command. sql: %r", sql)
                for result in special.execute(cur, sql):
                    yield result
            except special.CommandNotFound:  # Regular SQL
                _logger.debug("Regular sql statement. sql: %r", sql)
                cur.execute(sql)
                
                # Intercept the results to format nested types
                title, rows, headers, status = self.get_result(cur)
                
                if rows:
                    import json
                    cleaned_rows = []
                    for row in rows:
                        # Convert dicts and lists to JSON strings. 
                        # We use default=str to safely serialize any nested dates/UUIDs.
                        cleaned_row = tuple(
                            json.dumps(val, default=str) if isinstance(val, (dict, list)) else val 
                            for val in row
                        )
                        cleaned_rows.append(cleaned_row)
                    yield (title, cleaned_rows, headers, status)
                else:
                    yield (title, rows, headers, status)


    def get_result(self, cursor):
        """Get the current result's data from the cursor."""
        title = headers = None

        # cursor.description is not None for queries that return result sets,
        # e.g. SELECT.
        if cursor.description is not None:
            headers = [x[0] for x in cursor.description]
            status = "{0} row{1} in set"
            cursor = cursor.fetchall()
            rowcount = len(cursor)
        else:
            _logger.debug("No rows in result.")
            status = "Query OK, {0} row{1} affected"
            rowcount = 0 if cursor.rowcount == -1 else cursor.rowcount
            cursor = None

        status = status.format(rowcount, "" if rowcount == 1 else "s")

        return (title, cursor, headers, status)

    def tables(self):
        """Yields table names"""

        with closing(self.conn.cursor()) as cur:
            _logger.debug("Tables Query. sql: %r", self.tables_query)
            cur.execute(self.tables_query)
            for row in cur.fetchall():
                yield row

    def table_columns(self):
        """Yields (table_name, column_name) pairs, including nested struct fields."""
        import logging
        
        with closing(self.conn.cursor()) as cur:
            cur.execute(self.table_columns_query)
            for row in cur.fetchall():
                table_name = row[0]
                col_name = row[1]
                data_type = row[2] if len(row) > 2 else ""

                # 1. Always yield the base column
                yield table_name, col_name

                # 2. Parse STRUCT fields safely
                if data_type and data_type.upper().startswith('STRUCT'):
                    try:
                        # Extract everything inside STRUCT(...)
                        inner_struct = data_type[7:-1]
                        
                        # Split by comma and grab the field names
                        for field_def in inner_struct.split(','):
                            field_def = field_def.strip()
                            if not field_def: 
                                continue
                                
                            # Grab the first word and strip any quotes
                            field_name = field_def.split(' ')[0].replace('"', '').replace("'", "")
                        
                            # Just yield the standard completion
                            yield table_name, f"{col_name}.{field_name}"

                            # THE PSEUDO-TABLE HACK (This makes the dot work!)
                            # Registers the column as a table so the SQL parser finds it.
                            yield col_name, field_name
                            
                    except Exception as e:
                        logging.debug(f"Failed to parse STRUCT for autocomplete: {e}")

    def databases(self):
        if not self.conn:
            return

        with closing(self.conn.cursor()) as cur:
            _logger.debug("Databases Query. sql: %r", self.databases_query)
            for row in cur.execute(self.databases_query).fetchall():
                yield row[1]

    def functions(self):
        """Yields strings of function names"""
        import logging
        
        with closing(self.conn.cursor()) as cur:
            try:
                # No string formatting (no % self.dbname)
                cur.execute(self.functions_query)
                for row in cur.fetchall():
                    # MUST return just the string, not a tuple!
                    if row and row[0]:
                        yield row[0]
            except Exception as e:
                logging.debug(f"Failed to fetch DuckDB functions: {e}")

    def show_candidates(self):
        with closing(self.conn.cursor()) as cur:
            _logger.debug("Show Query. sql: %r", self.show_candidates_query)
            try:
                cur.execute(self.show_candidates_query)
            except Exception as e:
                _logger.error("No show completions due to %r", e)
                yield ""
            else:
                for row in cur.fetchall():
                    yield (row[0].split(None, 1)[-1],)

    def server_type(self):
        self._server_type = ("sqlite3", "3")
        return self._server_type

    def get_connection_id(self):
        if not self.connection_id:
            self.reset_connection_id()
        return self.connection_id

    def reset_connection_id(self):
        # Remember current connection id
        _logger.debug("Get current connection id")
        # res = self.run('select connection_id()')
        self.connection_id = uuid.uuid4()
        # for title, cur, headers, status in res:
        #     self.connection_id = cur.fetchone()[0]
        _logger.debug("Current connection id: %s", self.connection_id)
