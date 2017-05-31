import yaml
from contextlib import closing

from io import TextIOWrapper
from pkg_resources import resource_stream

import json
from jsonschema import validate


DEFAULT_SCHEMA = 'public'


class PgDatabase:
    def __init__(self):
        self.name = None
        self.extensions = []
        self.schemas = {}

    def register_schema(self, name):
        if name in self.schemas:
            return self.schemas.get(name)
        else:
            schema = PgSchema(name)

            self.schemas[name] = schema

            return schema


def validate_schema(data):
    with resource_stream(__name__, 'spec.schema') as schema_stream:
        with TextIOWrapper(schema_stream) as text_stream:
            schema = json.load(text_stream)

    validate(data, schema)

    return data


def load(infile):
    data = yaml.load(infile)

    validate_schema(data)

    version = data.get('version', '1')

    if version != '1':
        raise Exception('Unsupported format version: {}'.format(version))

    database = PgDatabase()

    database.extensions = data.get('extensions', [])

    types = [
        load_type(database, type_data)
        for type_data in data.get('types', [])
    ]

    objects = [
        load_object(database, object_data)
        for object_data in data['objects']
    ]

    tables = [object for object in objects if type(object) is PgTable]

    return database


def load_type(database, type_data):
    type_type, object_data = next(iter(type_data.items()))

    if type_type == 'enum':
        return PgEnum.load(database, object_data)
    else:
        raise Exception('Unsupported type: {}'.format(type_type))


def load_object(database, object_data):
    object_type, object_data = next(iter(object_data.items()))

    if object_type == 'table':
        return PgTable.load(database, object_data)
    else:
        raise Exception('Unsupported object type: {}'.format(object_type))


class PgSchema:
    def __init__(self, name):
        self.name = name
        self.types = []
        self.tables = []


class PgTable:
    def __init__(self, schema, name, columns):
        self.schema = schema
        self.name = name
        self.columns = columns
        self.primary_key = None
        self.foreign_keys = []
        self.unique = None
        self.check = None
        self.description = None

    @staticmethod
    def load(database, data):
        schema = database.register_schema(data.get('schema', DEFAULT_SCHEMA))

        table = PgTable(
            schema,
            data['name'],
            [
                PgColumn.load(database, column_data)
                for column_data in data['columns']
            ]
        )

        table.primary_key = data.get('primary_key')

        table.unique = data.get('unique')

        table.check = data.get('check')

        table.exclude = data.get('exclude')

        table.foreign_keys = [
            foreign_key
            for foreign_key in data.get('foreign_keys', [])
        ]

        schema.tables.append(table)

        return table


class PgColumn:
    def __init__(self, name, data_type):
        self.name = name
        self.data_type = data_type
        self.nullable = False
        self.description = None
        self.default = None

    @staticmethod
    def load(database, data):
        column = PgColumn(
            data['name'],
            data['data_type']
        )

        column.description = data.get('description')
        column.nullable = data.get('nullable', True)
        column.default = data.get('default', None)

        return column


class PgEnum:
    def __init__(self, schema, name, values):
        self.schema = schema
        self.name = name
        self.values = values

    @staticmethod
    def load(database, data):
        schema = database.register_schema(data['schema'])

        enum = PgEnum(
            schema,
            data['name'],
            data['values']
        )

        schema.types.append(enum)

        return enum
