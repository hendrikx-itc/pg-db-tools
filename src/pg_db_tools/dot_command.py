import argparse
import sys
from io import TextIOWrapper

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
        '--output-file', '-o', help='write output to file'
    )
    parser_dot.add_argument(
        '--out-encoding', help='encoding for output file'
    )
    parser_dot.add_argument(
        '--href-prefix', help='prefix to use for hrefs on table nodes',
        default='#'
    )

    parser_dot.set_defaults(cmd=dot_command)


def dot_command(args):
    if args.output_file:
        # Open file in binary mode because encoding is configured later
        out_file = open(args.output_file, 'wb')
    else:
        # Get binary raw buffer for stdout because encoding is configured later
        out_file = sys.stdout.buffer

    out_file = TextIOWrapper(out_file, args.out_encoding)

    data = load(args.infile)

    renderer = DotRenderer()
    renderer.href_prefix = args.href_prefix
    renderer.render(out_file, data)
