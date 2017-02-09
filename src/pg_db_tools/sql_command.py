import argparse
import codecs
import sys

from pg_db_tools.pg_types import load
from pg_db_tools.sql_renderer import SqlRenderer


def setup_command_parser(subparsers):
    parser_sql = subparsers.add_parser('sql', help='command for generating SQL')

    parser_sql.add_argument('infile', type=argparse.FileType('r', encoding='utf-8'))
    parser_sql.add_argument(
        '--output-file', '-o', help='write output to file',
        default=codecs.getwriter('utf-8')(sys.stdout.detach())
    )
    parser_sql.add_argument(
        '--if-not-exists', default=False, action='store_true',
        help='create database objects only if they don''t exist yet'
    )

    parser_sql.set_defaults(cmd=sql_command)


def sql_command(args):
    data = load(args.infile)

    renderer = SqlRenderer()
    renderer.if_not_exists = args.if_not_exists

    rendered_chunks = renderer.render_chunks(data)

    args.output_file.writelines(rendered_chunks)
