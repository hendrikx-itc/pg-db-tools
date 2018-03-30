import copy
from contextlib import closing
import json
from io import TextIOWrapper
from collections import OrderedDict

import itertools
from pkg_resources import resource_stream
import yaml
from jsonschema import validate


DEFAULT_SCHEMA = 'public'


class PgDatabase:
    def __init__(self):
        self.extensions = []
        self.schemas = {}

    @staticmethod
    def load_from_db(conn, include_schemas):
        database = PgDatabase()

        query = (
            "SELECT pg_namespace.oid "
            "FROM pg_namespace "
            "WHERE pg_namespace.nspname = ANY(%s)"
        )

        query_args = (include_schemas,)

        with closing(conn.cursor()) as cursor:
            cursor.execute(query, query_args)

            rows = cursor.fetchall()

        database.schemas = {
            schema.name: schema
            for schema in (PgSchema.load_from_db(conn, oid) for oid, in rows)
        }

        return database

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

    def to_json(self):
        return OrderedDict(
            objects=list(itertools.chain(*(
                schema.to_json()
                for schema in self.schemas.values()
            )))
        )


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
    elif object_type == 'function':
        return PgFunction.load(database, object_data)
    else:
        raise Exception('Unsupported object type: {}'.format(object_type))


class PgSchema:
    def __init__(self, name):
        self.name = name
        self.types = []
        self.tables = []
        self.functions = []

    @staticmethod
    def load_from_db(conn, oid):
        query = (
            "SELECT pg_namespace.nspname "
            "FROM pg_namespace "
            "WHERE oid = %s"
        )

        query_args = (oid,)

        with closing(conn.cursor()) as cursor:
            cursor.execute(query, query_args)

            name, = cursor.fetchone()

        schema = PgSchema(name)
        schema.tables = PgSchema.load_tables(conn, schema, oid)
        schema.functions = PgSchema.load_functions(conn, schema, oid)

        return schema

    @staticmethod
    def load_tables(conn, schema, schema_oid):
        query = (
            "SELECT pg_class.oid "
            "FROM pg_class "
            "WHERE relkind = %s AND pg_class.relnamespace = %s"
        )

        query_args = ('r', schema_oid)

        with closing(conn.cursor()) as cursor:
            cursor.execute(query, query_args)

            rows = cursor.fetchall()

        return [
            PgTable.load_from_db(conn, schema, oid) for oid, in rows
        ]

    @staticmethod
    def load_functions(conn, schema, schema_oid):
        query = (
            "SELECT oid "
            "FROM pg_proc "
            "WHERE pronamespace = %s"
        )

        query_args = (schema_oid,)

        with closing(conn.cursor()) as cursor:
            cursor.execute(query, query_args)

            rows = cursor.fetchall()

        return [
            PgFunction.load_from_db(conn, schema, oid) for oid, in rows
        ]

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

    def to_json(self):
        return list(itertools.chain(
            (
                OrderedDict([('table', table.to_json())])
                for table in self.tables
            ),
            (
                OrderedDict([('function', func.to_json())])
                for func in self.functions
            )
        ))


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
    def load_from_db(conn, schema, oid):
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
            'SELECT attname, pg_type.typname '
            'FROM pg_attribute '
            'JOIN pg_type ON pg_type.oid = pg_attribute.atttypid '
            'WHERE attrelid = %s AND attnum > 0 AND not attisdropped'
        )
        query_args = (table_oid,)

        with closing(conn.cursor()) as cursor:
            cursor.execute(query, query_args)

            rows = cursor.fetchall()

        return [
            PgColumn(attname, PgDataType(data_type))
            for attname, data_type in rows
        ]

    @staticmethod
    def load(database, data):
        schema = database.register_schema(data.get('schema', DEFAULT_SCHEMA))

        table = PgTable(
            schema,
            data['name'],
            [
                PgColumn.load(column_data)
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

    def to_json(self):
        attributes = [
            ('name', self.name),
            ('schema', self.schema.name),
            ('columns', [column.to_json() for column in self.columns])
        ]

        if self.primary_key is not None:
            attributes.append(('primary_key', self.primary_key.to_json()))

        if len(self.foreign_keys) > 0:
            attributes.append(
                ('foreign_keys', [foreign_key.to_json() for foreign_key in self.foreign_keys])
            )

        return OrderedDict(attributes)


class PgPrimaryKey:
    def __init__(self, name, columns):
        self.name = name
        self.columns = columns

    def to_json(self):
        return OrderedDict([
            ('name', self.name),
            ('columns', self.columns)
        ])

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

    def to_json(self):
        attributes = [
            ('name', self.name),
            ('data_type', self.data_type.to_json()),
            ('nullable', self.nullable)
        ]

        if self.description is not None:
            attributes.append(('description', self.description))

        if self.default is not None:
            attributes.append(('default', self.default))

        return OrderedDict(attributes)

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

    def to_json(self):
        return OrderedDict([
            ('name', self.name),
            ('columns', self.columns),
            ('references', OrderedDict([
                ('table', OrderedDict([
                    ('name', self.ref_table_name),
                    ('schema', self.ref_schema_name)
                ])),
                ('columns', self.ref_columns)
            ]))
        ])

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


class PgFunction:
    def __init__(self, schema, name, arguments, return_type):
        self.schema = schema
        self.name = name
        self.arguments = arguments
        self.return_type = return_type
        self.src = None
        self.language = None
        self.description = None

    @staticmethod
    def load(database, data):
        schema = database.register_schema(data['schema'])

        pg_function = PgFunction(
            schema,
            data['name'],
            [PgArgument.from_json(argument) for argument in data['arguments']],
            data['return_type']
        )

        pg_function.language = data.get('language')
        pg_function.src = PgSourceCode(data['source'])
        pg_function.description = data.get('description')

        schema.functions.append(pg_function)

        return pg_function

    def to_json(self):
        attributes = [
            ('name', self.name),
            ('schema', self.schema.name),
            ('return_type', self.return_type),
            ('language', self.language),
            ('arguments', [
                argument.to_json()
                for argument
                in self.arguments
            ])
        ]

        if self.description is not None:
            attributes.append(
                ('description', self.description)
            )

        attributes.append(('source', self.src))

        return OrderedDict(attributes)

    @staticmethod
    def load_from_db(conn, schema, oid):
        query = (
            'SELECT proname, return_type.typname, pg_language.lanname, prosrc, description '
            'FROM pg_proc '
            'JOIN pg_language ON pg_language.oid = pg_proc.prolang '
            'JOIN pg_type AS return_type ON return_type.oid = pg_proc.prorettype '
            'LEFT JOIN pg_description ON pg_description.objoid = pg_proc.oid '
            'WHERE pg_proc.oid = %s'
        )

        query_args = (oid, )

        with closing(conn.cursor()) as cursor:
            cursor.execute(query, query_args)

            name, return_type, language, src, description = cursor.fetchone()

        arguments = PgFunction.load_args_from_db(conn, oid)

        pg_function = PgFunction(schema, name, arguments, return_type)
        pg_function.language = language
        pg_function.src = PgSourceCode(src.strip())

        if description is not None:
            pg_function.description = PgDescription(description)

        return pg_function

    @staticmethod
    def load_args_from_db(conn, oid):
        query = (
            'select array_agg(pg_type.typname), array_agg(proc.proargname) '
            'from ('
            'select oid, unnest(proargtypes) as proargtype, unnest(proargnames) proargname '
            'from pg_proc'
            ') proc '
            'join pg_type on pg_type.oid = proc.proargtype '
            'where proc.oid = %s '
            'group by proc.oid'
        )

        query_args = (oid, )

        with closing(conn.cursor()) as cursor:
            cursor.execute(query, query_args)

            if cursor.rowcount:
                arg_types, arg_names = cursor.fetchone()

                return [
                    PgArgument(arg_name, PgDataType(arg_type), None, None)
                    for arg_name, arg_type
                    in zip(arg_names, arg_types)
                ]
            else:
                return []


data_type_mapping = {
    'int2': 'smallint',
    'int4': 'integer',
    'int8': 'bigint'
}


class PgDataType:
    def __init__(self, name):
        self.name = name

    def to_json(self):
        return str(self)

    def __str__(self):
        return data_type_mapping.get(self.name, self.name)


class PgSourceCode(str):
    pass


class PgDescription(str):
    pass


class PgArgument:
    def __init__(self, name, data_type, mode, default):
        self.name = name
        self.data_type = data_type
        self.mode = mode
        self.default = default

    @staticmethod
    def from_json(data):
        return PgArgument(
            data['name'],
            PgDataType(data['data_type']),
            data.get('mode'),
            data.get('default')
        )

    def to_json(self):
        attributes = [
            ('name', self.name),
            ('data_type', self.data_type.to_json())
        ]

        return OrderedDict(attributes)
