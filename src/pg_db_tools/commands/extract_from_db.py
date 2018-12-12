"""
Provides the 'from-db' sub-command including argument parsing
"""
import sys
from contextlib import closing
from collections import OrderedDict
import json

import psycopg2
import yaml

from pg_db_tools.pg_types import PgDatabase, PgSourceCode, PgDescription, \
    PgViewQuery


def setup_command_parser(subparsers):
    """
    Sets up a new sub parser for the from_db command and adds it to the
    provided subparsers
    """
    parser_extract = subparsers.add_parser(
        'from-db', help='command for extracting schema from live database'
    )

    parser_extract.add_argument(
        '--format', default='yaml', choices=['yaml', 'json'],
        help='format of output'
    )

    parser_extract.add_argument(
        '--owner', help='filter objects owned by specified user'
    )

    parser_extract.add_argument(
        'schemas', nargs='*', default=None, type=str, help='list of schemas to extract'
    )

    parser_extract.set_defaults(cmd=from_db_command)


def from_db_command(args):
    """
    Entry point for the from_db sub-command after parsing the arguments
    """
    with closing(psycopg2.connect('')) as conn:
        database = PgDatabase.load_from_db(conn)

    try:
        formatters[args.format](database.to_json(args.schemas))
    except KeyError:
        raise Exception('unsupported format: {}'.format(args.format))


def format_json(data):
    json.dump(data, sys.stdout, indent=2)


def format_yaml(data):
    yaml.SafeDumper.add_representer(
        OrderedDict,
        lambda dumper, value: represent_odict(
            dumper, u'tag:yaml.org,2002:map',
            value)
    )
    yaml.SafeDumper.add_representer(PgSourceCode, source_code_representer)
    yaml.SafeDumper.add_representer(PgDescription, description_representer)
    yaml.SafeDumper.add_representer(PgViewQuery, view_query_representer)

    yaml.safe_dump(data, sys.stdout, default_flow_style=False)


formatters = {
    'json': format_json,
    'yaml': format_yaml
}


def source_code_representer(dumper, data):
    return dumper.represent_scalar('tag:yaml.org,2002:str', data, style='|')


def view_query_representer(dumper, data):
    return dumper.represent_scalar('tag:yaml.org,2002:str', data, style='|')


def description_representer(dumper, data):
    return dumper.represent_scalar('tag:yaml.org,2002:str', data, style='|')


def represent_odict(dump, tag, mapping, flow_style=None):
    """Like BaseRepresenter.represent_mapping, but does not issue the sort().
    """
    value = []
    node = yaml.MappingNode(tag, value, flow_style=flow_style)
    if dump.alias_key is not None:
        dump.represented_objects[dump.alias_key] = node
    best_style = True
    if hasattr(mapping, 'items'):
        mapping = mapping.items()
    for item_key, item_value in mapping:
        node_key = dump.represent_data(item_key)
        node_value = dump.represent_data(item_value)
        if not (isinstance(node_key, yaml.ScalarNode) and not node_key.style):
            best_style = False
        if not (isinstance(node_value, yaml.ScalarNode) and not
                node_value.style):
            best_style = False
        value.append((node_key, node_value))
    if flow_style is None:
        if dump.default_flow_style is not None:
            node.flow_style = dump.default_flow_style
        else:
            node.flow_style = best_style
    return node
