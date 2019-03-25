"""
Provides the 'generate' sub-command including argument parsing
"""
import errno
import os

from pg_db_tools.pg_types import load
from pg_db_tools.rst_renderer import render_rst_directory


def setup_command_parser(subparsers):
    """
    Sets up a new sub parser for the generate command and adds it to the
    provided subparsers
    """
    parser = subparsers.add_parser(
        'generate', help='generate documentation source files'
    )

    parser.add_argument(
        'schema', default='schema.yml', help='schema definition file'
    )

    parser.add_argument(
        'directory', default='doc', help='target directory for documentation'
    )

    parser.set_defaults(cmd=init_command)


def init_command(args):
    """
    Entry point for the init sub-command after parsing the arguments
    """
    with open(args.schema) as infile:
        data = load(infile)

    schema_directory = os.path.join(args.directory, 'schema')

    try:
        os.makedirs(schema_directory)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise

    render_rst_directory(schema_directory, data)
