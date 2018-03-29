import copy
from contextlib import closing
import json
from io import TextIOWrapper

import itertools
from pkg_resources import resource_stream
import yaml
from jsonschema import validate


DEFAULT_SCHEMA = 'public'


class PgDatabase:
    def __init__(self):
        self.extensions = []
        self.schemas = {}

    def register_schema(self, name):
        if name in self.schemas:
            return self.schemas.get(name)
        else:
            schema = PgSchema(name)

            self.schemas[name] = schema

            return schema

    def filter_objects(self, database_filter):
        database = PgDatabase()
        database.extensions = copy.copy(self.extensions)

        database.schemas = {
            name: schema.filter_objects(database_filter)
            for name, schema in self.schemas.items()
        }

        return database


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

    def filter_objects(self, database_filter):
        """
        Return new PgSchema object containing only filtered types and tables
        """
        schema = PgSchema(self.name)

        schema.types = list(
            filter(database_filter.include_type, self.types)
        )

        schema.tables = list(
            filter(database_filter.include_table, self.tables)
        )

        return schema


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

    def __str__(self):
        return '"{}"."{}"'.format(self.schema.name, self.name)

    @staticmethod
    def load_from_db(database, conn, oid):
        query = (
            'SELECT nspname, relname '
            'FROM pg_class '
            'JOIN pg_namespace ON pg_namespace.oid = pg_class.relnamespace '
            'WHERE pg_class.oid = %s'
        )
        query_args = (oid,)

        with closing(conn.cursor()) as cursor:
            cursor.execute(query, query_args)

            schema_name, table_name = cursor.fetchone()

        schema = database.register_schema(schema_name)

        table = PgTable(
            schema,
            table_name,
            PgTable.load_columns_from_db(conn, oid)
        )

        table.primary_key = PgPrimaryKey.load_from_db(conn, oid)
        table.foreign_keys = PgForeignKey.load_from_db_for_table(conn, oid)

        return table

    @staticmethod
    def load_columns_from_db(conn, table_oid):
        query = (
            'SELECT attname, format_type(atttypid, atttypmod) '
            'FROM pg_attribute '
            'WHERE attrelid = %s AND attnum > 0'
        )
        query_args = (table_oid,)

        with closing(conn.cursor()) as cursor:
            cursor.execute(query, query_args)

            rows = cursor.fetchall()

        return [
            PgColumn(attname, data_type)
            for attname, data_type in rows
        ]

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

        primary_key_data = data.get('primary_key')

        if primary_key_data is not None:
            table.primary_key = PgPrimaryKey.load(primary_key_data)

        table.unique = data.get('unique')

        table.check = data.get('check')

        table.exclude = data.get('exclude')

        table.foreign_keys = [
            foreign_key
            for foreign_key in data.get('foreign_keys', [])
        ]

        schema.tables.append(table)

        return table

    def to_yaml(self):
        parts = [
            (
                '  - table:\n',
                '      name: {}\n'.format(self.name),
                '      schema: {}\n'.format(self.schema.name),
                '      columns:\n'
            )
        ]

        parts.extend(
            (
                '        - name: {}\n'.format(column.name),
                '          data_type: {}\n'.format(column.data_type)
            )
            for column in self.columns
        )

        if self.primary_key is not None:
            parts.append(
                (
                    '      primary_key:\n',
                    '        name: {}\n'.format(self.primary_key.name),
                    '        columns: [{}]\n'.format(', '.join('"{}"'.format(name) for name in self.primary_key.columns))
                )
            )

        if self.foreign_keys:
            parts.append(
                (
                    '      foreign_keys:\n'
                )
            )
            for foreign_key in self.foreign_keys:
                parts.append(
                    (
                        '        - name: {}\n'.format(foreign_key.name),
                        '          columns: [{}]\n'.format(', '.join('"{}"'.format(column) for column in foreign_key.columns)),
                        '          references:\n',
                        '            table:\n',
                        '              name: {}\n'.format(foreign_key.ref_table_name),
                        '              schema: {}\n'.format(foreign_key.ref_schema_name),
                        '            columns: [{}]\n'.format(', '.join('"{}"'.format(column) for column in foreign_key.ref_columns))
                    )
                )

        return itertools.chain(*parts)


class PgPrimaryKey:
    def __init__(self, name, columns):
        self.name = name
        self.columns = columns

    @staticmethod
    def load_from_db(conn, table_oid):
        query = (
            'SELECT conname '
            'FROM pg_constraint '
            'WHERE contype = \'p\' AND conrelid = %s'
        )

        query_args = (table_oid,)

        with closing(conn.cursor()) as cursor:
            cursor.execute(query, query_args)

            if cursor.rowcount > 0:
                name, = cursor.fetchone()

                return PgPrimaryKey(
                    name,
                    PgPrimaryKey.load_columns_from_db(conn, table_oid)
                )
            else:
                return None

    @staticmethod
    def load_columns_from_db(conn, table_oid):
        query = (
            'SELECT attname '
            'FROM pg_constraint '
            'JOIN pg_attribute ON pg_attribute.attrelid = conindid '
            'WHERE contype = \'p\' AND conrelid = %s'
        )

        query_args = (table_oid,)

        with closing(conn.cursor()) as cursor:
            cursor.execute(query, query_args)

            return [
                attname for attname, in cursor.fetchall()
            ]

    @staticmethod
    def load(data):
        return PgPrimaryKey(data.get('name'), data.get('columns'))


class PgColumn:
    def __init__(self, name, data_type):
        self.name = name
        self.data_type = data_type
        self.nullable = False
        self.description = None
        self.default = None

    @staticmethod
    def load(data):
        column = PgColumn(
            data['name'],
            data['data_type']
        )

        column.description = data.get('description')
        column.nullable = data.get('nullable', True)
        column.default = data.get('default', None)

        return column


class PgForeignKey:
    def __init__(self, name, columns, ref_table_name, ref_schema_name, ref_columns):
        self.name = name
        self.columns = columns
        self.ref_table_name = ref_table_name
        self.ref_schema_name = ref_schema_name
        self.ref_columns = ref_columns

    @staticmethod
    def load_from_db_for_table(conn, table_oid):
        query = (
            'SELECT oid '
            'FROM pg_constraint '
            'WHERE contype = \'f\' AND conrelid = %s'
        )

        query_args = (table_oid,)

        with closing(conn.cursor()) as cursor:
            cursor.execute(query, query_args)

            return [
                PgForeignKey.load_from_db(conn, foreign_key_oid)
                for foreign_key_oid, in cursor.fetchall()
            ]

    @staticmethod
    def load_from_db(conn, oid):
        query = (
            'SELECT conname, array_agg(col.attname), pg_class.relname, pg_namespace.nspname, array_agg(refcol.attname) '
            'FROM pg_constraint '
            'JOIN pg_class ON pg_class.oid = confrelid '
            'JOIN pg_namespace ON pg_class.relnamespace = pg_namespace.oid '
            'JOIN pg_attribute col ON col.attrelid = pg_constraint.conrelid AND col.attnum = ANY(conkey) '
            'JOIN pg_attribute refcol ON refcol.attrelid = pg_constraint.confrelid AND refcol.attnum = ANY(confkey) '
            'WHERE contype = \'f\' AND pg_constraint.oid = %s '
            'GROUP BY conname, pg_class.relname, pg_namespace.nspname'
        )

        query_args = (oid,)

        with closing(conn.cursor()) as cursor:
            cursor.execute(query, query_args)

            name, columns, ref_table_name, ref_schema_name, ref_columns = cursor.fetchone()

        return PgForeignKey(name, columns, ref_table_name, ref_schema_name, ref_columns)

    @staticmethod
    def load(data):
        return PgForeignKey(
            data['name'],
            data['columns'],
            data['references']['table']['name'],
            data['references']['table']['schema'],
            data['references']['columns']
        )


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
