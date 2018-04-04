import copy
from contextlib import closing
import json
from io import TextIOWrapper
from collections import OrderedDict
from operator import itemgetter
import itertools

from pkg_resources import resource_stream
import yaml
from jsonschema import validate


DEFAULT_SCHEMA = 'public'


class PgDatabase:
    def __init__(self):
        self.extensions = []
        self.schemas = {}
        self.types = {}
        self.objects = []
        self.views = {}

    @staticmethod
    def load(data):
        database = PgDatabase()

        database.extensions = data.get('extensions', [])

        database.objects = [
            load_object(database, object_data)
            for object_data in data['objects']
        ]

        return database

    @staticmethod
    def load_from_db(conn):
        database = PgDatabase()

        database.schemas = PgSchema.load_all_from_db(conn, database)
        database.types = PgType.load_all_from_db(conn, database)

        for pg_type in database.types.values():
            pg_type.schema.types.append(pg_type)

        database.tables = PgTable.load_all_from_db(conn, database)

        for pg_table in database.tables.values():
            pg_table.schema.tables.append(pg_table)

        PgPrimaryKey.load_all_from_db(conn, database)

        database.views = PgView.load_all_from_db(conn, database)

        for pg_view in database.views.values():
            pg_view.schema.views.append(pg_view)

        database.functions = PgFunction.load_all_from_db(conn, database)

        for pg_function in database.functions.values():
            pg_function.schema.functions.append(pg_function)

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
        def filter_schema(schema):
            ok = schema.name not in ['pg_catalog', 'information_schema', 'pg_toast', 'pg_temp_1', 'pg_toast_temp_1', 'public', 'dep_recurse']

            return ok

        return OrderedDict(
            objects=list(itertools.chain(*(
                schema.to_json()
                for schema in self.schemas.values() if filter_schema(schema)
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

    database.objects = [
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


class PgSchema:
    def __init__(self, name):
        self.name = name
        self.types = []
        self.tables = []
        self.functions = []
        self.views = []

    @staticmethod
    def load_all_from_db(conn, database):
        query = (
            "SELECT pg_namespace.oid, pg_namespace.nspname "
            "FROM pg_namespace "
        )

        query_args = tuple()

        with closing(conn.cursor()) as cursor:
            cursor.execute(query, query_args)

            rows = cursor.fetchall()

        return {
            oid: PgSchema(name)
            for oid, name in rows
        }

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
            ),
            (
                OrderedDict([('view', view.to_json())])
                for view in self.views
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
    def load_all_from_db(conn, database):
        query = (
            'SELECT oid, relnamespace, relname '
            'FROM pg_class '
            'WHERE relkind = \'r\''
        )
        query_args = tuple()

        with closing(conn.cursor()) as cursor:
            cursor.execute(query, query_args)

            rows = cursor.fetchall()

        tables = {
            oid: PgTable(database.schemas[namespace_oid], name, [])
            for oid, namespace_oid, name in rows
        }

        query = (
            'SELECT attrelid, attname, atttypid, pg_description.description '
            'FROM pg_attribute '
            'LEFT JOIN pg_description ON pg_description.objoid = pg_attribute.attrelid AND pg_description.objsubid = pg_attribute.attnum '
            'WHERE attnum > 0'
        )

        query_args = tuple()

        with closing(conn.cursor()) as cursor:
            cursor.execute(query, query_args)

            column_rows = cursor.fetchall()

        sorted_column_rows = sorted(column_rows, key=itemgetter(0))

        for key, group in itertools.groupby(sorted_column_rows, key=itemgetter(0)):
            table = tables.get(key)

            if table is not None:
                table.columns = [
                    PgColumn(column_name, database.types[column_type_oid])
                    for table_oid, column_name, column_type_oid, column_description in group
                ]

        return tables

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
    def load_all_from_db(conn, database):
        query = (
            'SELECT conrelid, conname, array_agg(attname) '
            'FROM pg_constraint '
            'JOIN pg_attribute ON pg_attribute.attrelid = conindid '
            'WHERE contype = \'p\' '
            'GROUP BY conrelid, conname'
        )

        query_args = tuple()

        with closing(conn.cursor()) as cursor:
            cursor.execute(query, query_args)

            rows = cursor.fetchall()

        for table_oid, name, column_names in rows:
            table = database.tables[table_oid]

            table.primary_key = PgPrimaryKey(
                name,
                column_names
            )

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
            ('return_type', self.return_type.to_json()),
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
    def load_all_from_db(conn, database):
        query = (
            'SELECT pg_proc.oid, pronamespace, proname, prorettype, '
            'proargtypes, proallargtypes, proargmodes, proargnames, '
            'pg_language.lanname, prosrc, description '
            'FROM pg_proc '
            'JOIN pg_language ON pg_language.oid = pg_proc.prolang '
            'LEFT JOIN pg_description ON pg_description.objoid = pg_proc.oid'
        )

        query_args = tuple()

        with closing(conn.cursor()) as cursor:
            cursor.execute(query, query_args)

            rows = cursor.fetchall()

        def function_from_row(row):
            (
                oid, namespace_oid, name, return_type_oid, arg_type_oids_str,
                all_arg_type_oids, arg_modes, arg_names, language, src,
                description
            ) = row

            if arg_type_oids_str:
                arg_type_oids = list(map(int, arg_type_oids_str.split(' ')))
            else:
                arg_type_oids = []

            if all_arg_type_oids is None:
                all_arg_type_oids = arg_type_oids

            if arg_modes is None:
                arg_modes = len(arg_type_oids) * ['i']

            if arg_names is None:
                arg_names = len(all_arg_type_oids) * [None]

            arguments = [
                PgArgument(name, database.types[type_oid], arg_mode, None)
                for type_oid, name, arg_mode in zip(all_arg_type_oids, arg_names, arg_modes)
            ]

            pg_function = PgFunction(database.schemas[namespace_oid], name, arguments, database.types[return_type_oid])
            pg_function.language = language
            pg_function.src = PgSourceCode(src.strip())

            if description is not None:
                pg_function.description = PgDescription(description)

            return pg_function

        return {
            row[0]: function_from_row(row)
            for row in rows
        }


data_type_mapping = {
    'name': 'name',
    'int2': 'smallint',
    'int4': 'integer',
    'int8': 'bigint'
}


class PgTypeRef:
    def __init__(self, registry, ref):
        self.registry = registry
        self.ref = ref

    def dereference(self):
        return self.registry.get(self.ref)


class PgType:
    def __init__(self, schema, name):
        self.schema = schema
        self.name = name
        self.element_type = None

    @staticmethod
    def load_all_from_db(conn, database):
        query = (
            "SELECT oid, typname, typnamespace, typelem "
            "FROM pg_type"
        )

        with closing(conn.cursor()) as cursor:
            cursor.execute(query)

            rows = cursor.fetchall()

        pg_types = {}

        for oid, name, namespace_oid, element_oid in rows:
            pg_type = PgType(database.schemas[namespace_oid], name)

            if element_oid != 0:
                # Store a reference, because the targeted type may not be loaded yet
                pg_type.element_type = PgTypeRef(pg_types, element_oid)

            pg_types[oid] = pg_type

        # Dereference all references
        for pg_type in pg_types.values():
            if pg_type.element_type is not None:
                pg_type.element_type = pg_type.element_type.dereference()

        return pg_types

    def to_json(self):
        return str(self)

    def __str__(self):
        if self.schema is None or self.schema.name == 'pg_catalog':
            return data_type_mapping.get(self.name, self.name)
        else:
            if self.element_type is not None:
                return "{}[]".format(str(self.element_type))
            else:
                if self.schema is None:
                    return self.name
                else:
                    return '{}.{}'.format(self.schema.name, self.name)


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
            data.get('name'),
            PgType(None, data['data_type']),
            data.get('mode'),
            data.get('default')
        )

    def to_json(self):
        attributes = []

        if self.name is not None:
            attributes.append(('name', self.name))

        attributes.append(('data_type', self.data_type.to_json()))

        if self.mode is not None and self.mode != 'i':
            attributes.append(('mode', self.mode))

        if self.default is not None:
            attributes.append(('default', self.default))

        return OrderedDict(attributes)


class PgViewQuery(str):
    pass


class PgView:
    def __init__(self, schema, name, view_query):
        self.schema = schema
        self.name = name
        self.view_query = view_query

    @staticmethod
    def load_all_from_db(conn, database):
        query = (
            'SELECT oid, relnamespace, relname, pg_get_viewdef(oid) '
            'FROM pg_class '
            'WHERE relkind = \'v\''
        )
        query_args = tuple()

        with closing(conn.cursor()) as cursor:
            cursor.execute(query, query_args)

            rows = cursor.fetchall()

        views = {
            oid: PgView(database.schemas[namespace_oid], name, PgViewQuery(view_def))
            for oid, namespace_oid, name, view_def in rows
        }

        return views

    def to_json(self):
        attributes = [
            ('name', self.name),
            ('schema', self.schema.name),
            ('query', self.view_query)
        ]

        return OrderedDict(attributes)


object_loaders = {
    'table': PgTable.load,
    'function': PgFunction.load
}


def load_object(database, object_data):
    object_type, object_data = next(iter(object_data.items()))

    try:
        return object_loaders[object_type](database, object_data)
    except IndexError:
        raise Exception('Unsupported object type: {}'.format(object_type))
