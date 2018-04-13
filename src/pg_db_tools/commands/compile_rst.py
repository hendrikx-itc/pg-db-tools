import argparse
import codecs
import sys
from io import TextIOWrapper

from pg_db_tools.pg_types import load
from pg_db_tools.rst_renderer import render_rst_file


def setup_command_parser(subparsers):
    parser_dot = subparsers.add_parser(
        'rst', help='command for generating reStructuredText documentation'
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

    render_rst_file(out_file, data)
