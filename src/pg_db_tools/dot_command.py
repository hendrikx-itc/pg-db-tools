import argparse
import codecs
import sys

from pg_db_tools.pg_types import load
from pg_db_tools.dot_renderer import DotRenderer


def setup_command_parser(subparsers):
    parser_dot = subparsers.add_parser(
        'dot', help='command for generating Graphviz DOT'
    )

    parser_dot.add_argument(
        'infile', type=argparse.FileType('r', encoding='utf-8')
    )
    parser_dot.add_argument(
        '--output-file', '-o', help='write output to file', default=sys.stdout
    )
    parser_dot.add_argument(
        '--out-encoding', help='encoding for output file'
    )

    parser_dot.set_defaults(cmd=dot_command)


def dot_command(args):
    if args.out_encoding:
        out_file = codecs.getwriter(args.out_encoding)(
            args.output_file.detach()
        )
    else:
        out_file = args.output_file

    data = load(args.infile)

    renderer = DotRenderer()
    renderer.render(out_file, data)
