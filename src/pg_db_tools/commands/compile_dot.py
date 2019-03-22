"""
Provides the ``dot`` sub-command including argument parsing
"""
import argparse
import sys
from io import TextIOWrapper

from pg_db_tools.pg_types import load
from pg_db_tools.dot_renderer import DotRenderer
from pg_db_tools.object_filter import DatabaseFilter, TableExclusionFilter, \
    TableInclusionFilter


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
    parser_dot.add_argument(
        '--select-tables', nargs='*',
        help='list of tables to include in the output'
    )
    parser_dot.add_argument(
        '--exclude-tables', nargs='*',
        help='list of tables to exclude in the output'
    )

    parser_dot.set_defaults(cmd=dot_command)


def configure_out_file(filepath, encoding):
    """
    Return text file object (either normal file or stdout) with proper encoding
    configured
    """
    if filepath:
        # Open file in binary mode because encoding is configured later
        out_file = open(filepath, 'wb')
    else:
        # Get binary raw buffer for stdout because encoding is configured later
        out_file = sys.stdout.buffer

    return TextIOWrapper(out_file, encoding)


def setup_table_filters(select_tables, exclude_tables):
    table_filters = []

    if exclude_tables:
        table_filters.append(TableExclusionFilter(exclude_tables))

    if select_tables:
        table_filters.append(TableInclusionFilter(select_tables))

    return table_filters


def dot_command(args):
    database_filter = DatabaseFilter(
        table_filters=setup_table_filters(
            args.select_tables, args.exclude_tables
        ),
        type_filters=[]
    )

    out_file = configure_out_file(args.output_file, args.out_encoding)

    data = load(args.infile)
    data = data.filter_objects(database_filter)

    renderer = DotRenderer()
    renderer.href_prefix = args.href_prefix
    renderer.render(out_file, data)
