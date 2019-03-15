"""
Provides the 'sql' sub-command including argument parsing
"""
import argparse
import sys
from io import TextIOWrapper

from pg_db_tools.pg_types import load
from pg_db_tools.sql_renderer import SqlRenderer


def setup_command_parser(subparsers):
    """
    Sets up a new sub parser for the sql command and adds it to the provided
    subparsers
    """
    parser_sql = subparsers.add_parser(
        'sql', help='command for generating SQL'
    )

    parser_sql.add_argument(
        'infile', type=argparse.FileType('r', encoding='utf-8')
    )
    parser_sql.add_argument(
        '--output-file', '-o', help='write output to file'
    )
    parser_sql.add_argument(
        '--if-not-exists', default=False, action='store_true',
        help='create database objects only if they don''t exist yet'
    )
    parser_sql.add_argument(
        '--function', help='select specific function'
    )
    parser_sql.add_argument('--out-encoding', help='encoding for output file')

    parser_sql.set_defaults(cmd=sql_command)


def sql_command(args):
    """
    Entry point for the sql sub-command after parsing the arguments
    """
    if args.output_file:
        # Open file in binary mode because encoding is configured later
        out_file = open(args.output_file, 'wb')
    else:
        # Get binary raw buffer for stdout because encoding is configured later
        out_file = sys.stdout.buffer

    out_file = TextIOWrapper(out_file, args.out_encoding)

    data = load(args.infile)

    renderer = SqlRenderer()
    renderer.if_not_exists = args.if_not_exists

    rendered_chunks = renderer.render_chunks(data)

    out_file.writelines(rendered_chunks)
