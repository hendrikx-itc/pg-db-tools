import argparse
import codecs
import sys

from pg_db_tools.pg_types import load
from pg_db_tools.dot_renderer import DotRenderer


def setup_command_parser(subparsers):
    parser_dot = subparsers.add_parser('dot', help='command for generating Graphviz DOT')

    parser_dot.add_argument('infile', type=argparse.FileType('r', encoding='utf-8'))
    parser_dot.add_argument(
        '--output-file', '-o', help='write output to file', default=codecs.getwriter('utf-8')(sys.stdout)
    )

    parser_dot.set_defaults(cmd=dot_command)


def dot_command(args):
    data = load(args.infile)

    renderer = DotRenderer()
    renderer.render(args.output_file, data)
