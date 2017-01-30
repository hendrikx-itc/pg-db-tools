import argparse
import sys

import yaml

from pg_db_tools.sql_renderer import SqlRenderer


def setup_command_parser(subparsers):
    parser_sql = subparsers.add_parser('sql', help='command for generating SQL')

    parser_sql.add_argument('infile', type=argparse.FileType('r'))
    parser_sql.add_argument('--output-file', '-o', help='write output to file', default=sys.stdout)
    parser_sql.add_argument(
        '--if-not-exists', default=False, action='store_true',
        help='create database objects only if they don''t exist yet'
    )

    parser_sql.set_defaults(cmd=sql_command)


def sql_command(args):
    data = yaml.load(args.infile)

    renderer = SqlRenderer()
    renderer.if_not_exists = args.if_not_exists

    rendered_chunks = renderer.render(data)

    args.output_file.writelines(rendered_chunks)
