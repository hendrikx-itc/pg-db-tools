import argparse
import sys

import yaml

from pg_db_tools.sql_renderer import render_sql


def setup_command_parser(subparsers):
    parser_sql = subparsers.add_parser('sql', help='command for generating SQL')

    parser_sql.add_argument('infile', type=argparse.FileType('r'))
    parser_sql.add_argument('--output-file', '-o', help='write output to file', default=sys.stdout)

    parser_sql.set_defaults(cmd=sql_command)


def sql_command(args):
    data = yaml.load(args.infile)

    rendered_chunks = render_sql(data)

    args.output_file.writelines(rendered_chunks)
