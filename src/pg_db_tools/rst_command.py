import argparse

import sys
import yaml

from pg_db_tools.rst_renderer import render_rst


def setup_command_parser(subparsers):
    parser_dot = subparsers.add_parser('rst', help='command for generating reStructuredText documentation')

    parser_dot.add_argument('infile', type=argparse.FileType('r'))
    parser_dot.add_argument('--output-file', '-o', help='write output to file', default=sys.stdout)

    parser_dot.set_defaults(cmd=dot_command)


def dot_command(args):
    data = yaml.load(args.infile)

    rendered_chunks = render_rst(data)

    args.output_file.writelines(rendered_chunks)
