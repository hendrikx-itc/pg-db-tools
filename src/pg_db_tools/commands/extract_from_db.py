"""
Provides the 'sql' sub-command including argument parsing
"""
from contextlib import closing

import psycopg2

from pg_db_tools.pg_types import PgTable, PgDatabase


def setup_command_parser(subparsers):
    """
    Sets up a new sub parser for the from_db command and adds it to the provided
    subparsers
    """
    parser_sql = subparsers.add_parser(
        'from-db', help='command for extracting schema from live database'
    )

    parser_sql.set_defaults(cmd=from_db_command)


def from_db_command(args):
    """
    Entry point for the from_db sub-command after parsing the arguments
    """
    with closing(psycopg2.connect('')) as conn:
        load_schemas(conn)


def load_schemas(conn):
    database = PgDatabase()

    query = "SELECT oid, relname FROM pg_class WHERE relkind = %s"

    with closing(conn.cursor()) as cursor:
        cursor.execute(query, ('r',))

        rows = cursor.fetchall()

        for row in rows:
            PgTable()
            print(row)
