import argparse
import codecs

import sys
import yaml

from pg_db_tools.md_renderer import render_md


def setup_command_parser(subparsers):
    parser_dot = subparsers.add_parser(
        'md', help='command for generating Markdown documentation'
    )

    parser_dot.add_argument(
        'infile', type=argparse.FileType('r')
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

    data = yaml.load(args.infile)

    rendered_chunks = render_md(data)

    out_file.writelines(rendered_chunks)
