import logging
import psycopg
import psycopg.sql as sql

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DBTable:
    """ A class to represent a table object and its operations"""

    def __init__(self, host, database, user, password, table, primary_key):
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.table = table
        self.pk = primary_key
        # self.operation_type = operation_type
        self._conn = None
        self._cursor = None

    def _connect(self):
        """ Creates database connection and cursor"""
        if not self._conn:
            try:
                self._conn = psycopg.connect(
                    host=self.host,
                    dbname=self.database,
                    user=self.user
                )
                self._cursor = self._conn.cursor()
            except Exception as e:
                raise Exception(f"Failure connecting to database: {e}")
            return self._conn, self._cursor

    def _execute_query(self, query: sql.Composed, placeholder_val=None, query_type=None):
        """
        Executes query on the database
        Parameters:
            query: sql query
            placeholder_val: values to be used to filter results
            query_type: type of query operation
        Returns:
            All fetched results only in case of select operation
        """
        self._connect()  # Ensures connection is open
        with self._conn.cursor() as cur:
            try:
                logger.info(query.as_string(self._conn))
                if placeholder_val is None:
                    cur.execute(query)
                    self._conn.commit()
                else:
                    cur.execute(query, placeholder_val)
                    self._conn.commit()
                if query_type == "SELECT" or 'RETURNING' in query.as_string(self._conn):
                    return cur.fetchall()
            except Exception as e:
                self._conn.rollback()
                raise Exception(f"Error in query execution: {e}")

    def select(self, columns: list, where_clause_col: str = None, where_clause_val=None):
        """
        Performs select operation on the table.
        :param columns: list of columns, if specified
        :param where_clause_col: filter condition field
        :param where_clause_val: filter condition value
        :return: return value from execute query method
        """
        if columns == ['*']:
            columns_in_sql = sql.SQL('*')
        else:
            columns_in_sql = sql.SQL(', ').join(map(sql.Identifier, columns))

        if where_clause_col is None:
            select_query = sql.SQL("SELECT {} FROM {}").format(
                columns_in_sql,
                sql.Identifier(self.table)
            )
            return self._execute_query(select_query)
        else:
            select_query = sql.SQL("SELECT {} FROM {} WHERE {} = {}").format(
                columns_in_sql,
                sql.Identifier(self.table),
                sql.Identifier(where_clause_col),
                sql.Placeholder()
            )
            return self._execute_query(select_query, (where_clause_val,), "SELECT")

    def insert(self, column_values: dict):
        """
        Performs insert operation on the table.
        :param column_values: dictionary containing field, value pairs
        """
        insert_query = sql.SQL("INSERT INTO {}({}) VALUES ({}) RETURNING *").format(
            sql.Identifier(self.table),
            sql.SQL(', ').join(map(sql.Identifier, column_values.keys())),
            sql.SQL(', ').join(sql.Placeholder() * len(column_values.values()))
        )
        values_to_insert = tuple(column_values.values())
        return self._execute_query(insert_query, values_to_insert)

    def update(self, updates: dict, pk_value: str):
        """
        Performs update operation on the table.
        :param updates: dictionary of fields and corresponding values to be set
        :param pk_value: primary key condition value
        """
        set_clause = sql.SQL(', ').join(
            [sql.SQL("{} = {}").format(sql.Identifier(k), sql.Placeholder()) for k in updates.keys()])
        update_query = sql.SQL("UPDATE {} SET {} WHERE {} = {} RETURNING *").format(
            sql.Identifier(self.table),
            set_clause,
            sql.Identifier(self.pk),
            sql.Placeholder()
        )
        placeholder_values = list(updates.values())
        placeholder_values.append(pk_value)
        return self._execute_query(update_query, tuple(placeholder_values))

    def delete(self, where_clause_col: str = None, where_clause_val: str = None):
        if where_clause_col and where_clause_val:
            delete_query = sql.SQL("DELETE FROM {} WHERE {} = {} RETURNING *").format(
                sql.Identifier(self.table),
                sql.Identifier(where_clause_col),
                sql.Placeholder()
            )
            return self._execute_query(delete_query, (where_clause_val,))
        else:
            confirm = input(f"Are you sure you want to delete all records from {self.table}? (yes/no): ")
            if confirm == "yes":
                delete_query = sql.SQL("DELETE FROM {}").format(
                    sql.Identifier(self.table)
                )
                self._execute_query(delete_query)
            else:
                logger.info("Delete operation cancelled")
