"""
Provides the 'sql' sub-command including argument parsing
"""
import sys
from contextlib import closing

import psycopg2

from pg_db_tools.pg_types import PgTable, PgDatabase, PgSchema


def setup_command_parser(subparsers):
    """
    Sets up a new sub parser for the from_db command and adds it to the provided
    subparsers
    """
    parser_extract = subparsers.add_parser(
        'from-db', help='command for extracting schema from live database'
    )

    parser_extract.add_argument(
        'schemas', nargs='+', type=str, help='list of schemas to extract'
    )

    parser_extract.set_defaults(cmd=from_db_command)


def from_db_command(args):
    """
    Entry point for the from_db sub-command after parsing the arguments
    """
    with closing(psycopg2.connect('')) as conn:
        database = PgDatabase.load_from_db(conn, args.schemas)

    for line in database.to_yaml():
        sys.stdout.write(line)
