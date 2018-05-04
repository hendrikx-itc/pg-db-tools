import copy
from contextlib import closing
import json
from io import TextIOWrapper
from collections import OrderedDict
from operator import itemgetter
import itertools
import re

from pkg_resources import resource_stream
import yaml
from jsonschema import validate


DEFAULT_SCHEMA = 'public'
SILENT_SCHEMAS = [DEFAULT_SCHEMA, 'pg_catalog']
SKIPPED_SCHEMAS = [ 'pg_catalog', 'information_schema', 'pg_toast', 'pg_temp_1',
                    'pg_toast_temp_1', 'dep_recurse']

class PgDatabase:

    dependencyre_with_arguments = re.compile('(?s)([\w_]+"?\.[\w_]+"?)\(')
    dependencyre_without_arguments = re.compile('([\w_]+)"?\.([\w_]+)(?!\w)(?!"?\()')
    
    def __init__(self):
        self.extensions = []
        self.schemas = {}
        self.types = {}
        self.tables = {}
        self.composite_types = {}
        self.function = {}
        self.objects = []
        self.views = {}
        self.roles = {}
        self.triggers = {}
        self.sequences = {}
        self.casts = {}
        self.rows = []
        
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

        database.sequences = PgSequence.load_all_from_db(conn, database)

        for pg_sequence in database.sequences.values():
            if pg_sequence not in pg_sequence.schema.sequences:
                pg_sequence.schema.sequences.append(pg_sequence)

        database.types = PgType.load_all_from_db(conn, database)

        for pg_type in database.types.values():
            pg_type.schema.types.append(pg_type)

        database.enum_types = PgEnumType.load_all_from_db(conn, database)

        for pg_enum_type in database.enum_types.values():
            pg_enum_type.schema.enum_types.append(pg_enum_type)

        database.composite_types = PgCompositeType.load_all_from_db(
            conn, database
        )

        for pg_composite_type in database.composite_types.values():
            if pg_composite_type not in pg_composite_type.schema.composite_types:
                pg_composite_type.schema.composite_types.append(pg_composite_type)

        database.tables = PgTable.load_all_from_db(conn, database)

        for pg_table in database.tables.values():
            if pg_table not in pg_table.schema.tables:
                pg_table.schema.tables.append(pg_table)

        PgPrimaryKey.load_all_from_db(conn, database)

        database.views = PgView.load_all_from_db(conn, database)

        for pg_view in database.views.values():
            if pg_view not in pg_view.schema.views:
                pg_view.schema.views.append(pg_view)

        database.functions = PgFunction.load_all_from_db(conn, database)

        for pg_function in database.functions.values():
            if pg_function not in pg_function.schema.functions:
                pg_function.schema.functions.append(pg_function)

        database.aggregates = PgAggregate.load_all_from_db(conn, database)

        for pg_aggregate in database.aggregates.values():
            if pg_aggregate not in pg_aggregate.schema.aggregates:
                pg_aggregate.schema.aggregates.append(pg_aggregate)

        database.triggers = PgTrigger.load_all_from_db(conn, database)

        database.foreign_keys = PgForeignKey.load_all_from_db(conn, database)

        for pg_foreign_key in database.foreign_keys.values():
            pg_foreign_key.schema.foreign_keys.append(pg_foreign_key)

        database.casts = PgCast.load_all_from_db(conn, database)
            
        database.dependencies = PgDepend.load_all_from_db(conn, database)

        database.roles = PgRole.load_all_from_db(conn, database)

        PgIndex.load_all_from_db(conn, database)

        database.objects = list(database.schemas.values()) + list(database.roles.values()) +\
                           list(database.sequences.values()) + list(database.enum_types.values()) +\
                           list(database.composite_types.values()) + list(database.tables.values()) +\
                           list(database.functions.values()) + list(database.aggregates.values()) +\
                           list(database.views.values()) + list(database.triggers.values()) +\
                           list(database.casts.values())

        return database

    def register_schema(self, name):
        if name in self.schemas:
            return self.schemas.get(name)
        else:
            schema = PgSchema(name, self)
            self.schemas[name] = schema
            return schema

    def get_schema_by_name(self, name):
        schemas = [schema for schema in self.schemas.values() if schema.name == name]
        if schemas:
            return schemas[0]
        elif name in SILENT_SCHEMAS:
            return PgSchema(name, self)
        else:
            return None

    def filter_objects(self, database_filter):
        database = PgDatabase()
        database.extensions = copy.copy(self.extensions)

        database.schemas = {
            name: schema.filter_objects(database_filter)
            for name, schema in self.schemas.items()
        }

        return database

    def to_json(self, internal_order=False):
        objects_to_include = [object for object in self.objects if object.schema.name not in SKIPPED_SCHEMAS]
        for object in objects_to_include:
            object.build_dependencies()
        objects_included = []
        while objects_to_include:
            for object in objects_to_include:
                if not object.is_blocked(objects_to_include):
                    objects_included.append(object)
                    objects_to_include.remove(object)
                    break
            else:
                # all remaining objects are in a cycle or depend on a cycle; try to pick one that leads to
                # least chance of being problematic
                for object in objects_to_include:
                    if not object.is_blocked(objects_to_include, samenameblocks = False):
                        objects_included.append(object)
                        objects_to_include.remove(object)
                        break
                else:
                    # not able to find some lower-risk object, just take one at random
                    objects_included.append(objects_to_include[0])
                    objects_to_include = objects_to_include[1:]
                
        return OrderedDict(
            objects=[
                OrderedDict([(object.object_type, object.to_json())])
                for object in objects_included
            ]
        )

    def get_type_ref(self, typestring):
        if '.' in typestring:
            return PgTypeRef(self.register_schema(typestring.split('.', 1)[0]), typestring.split('.', 1)[1])
        else:
            return PgTypeRef(self.register_schema(DEFAULT_SCHEMA), typestring)

    def find_dependencies(self, text):
        dependencies = []
        remaining_text = text
        for fullname in self.dependencyre_with_arguments.findall(str(text)):
            loc = remaining_text.find(fullname + "(") + len(fullname)
            assert remaining_text[loc] == '('
            (schema_name, name) = fullname.replace('"', '').split('.')
            loc2 = loc
            depth = 1
            commas = 0
            while depth > 0 and loc2 < len(remaining_text):
                loc2 += 1
                if remaining_text[loc2] == '(':
                    depth += 1
                elif remaining_text[loc2] == ')':
                    depth -= 1
                elif remaining_text[loc2] == ',' and depth == 1:
                    commas += 1
            arguments = remaining_text[loc+1:loc2]
            schema = self.get_schema_by_name(schema_name)
            if arguments.strip():
                argument_number = commas + 1
            else:
                argument_number = 0
            if schema:
                for object in schema.getall(name):
                    if object.argument_number == argument_number:
                        dependencies.append(object)
            remaining_text = remaining_text[loc:]
        for (schema_name, name) in self.dependencyre_without_arguments.findall(str(text)):
            schema = self.get_schema_by_name(schema_name)
            if schema:
                for object in schema.getall(name):
                    dependencies.append(object)
        return dependencies
        

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

    database.objects = [
        load_object(database, object_data)
        for object_data in data['objects']
    ]

    return database


class PgObject:
    # abstract class, serving as a base class for the classes below
    def is_blocked(self, blockingobjects, samenameblocks = True):
        # is this object dependent on some object in blockingobjects
        # if samennameblocks is False, objects with the same name don't block
        if samenameblocks:
            return bool([object for object in self.dependencies if object in blockingobjects and object != self])
        else:
            return bool([object for object in self.dependencies if object in blockingobjects and object.name != self.name])

    def build_dependencies(self):
        self.dependencies = self.get_dependencies() + [self.schema]

    def get_dependencies(self):
        # To be overwritten in child classes. The objects this depends on.
        return []

    def dereference(self):
        return self

    @property
    def argument_number(self):
        try:
            return len(self.arguments)
        except AttributeError:
            return 0

    @property
    def database(self):
        return self.schema.database
        

class PgSchema(PgObject):
    def __init__(self, name, database):
        self.name = name
        self.types = []
        self.enum_types = []
        self.composite_types = []
        self.tables = []
        self.functions = []
        self.sequences = []
        self.views = []
        self.foreign_keys = []
        self.aggregates = []
        self._database = database
        self.schema = self
        self.object_type = 'schema'

    @property
    def database(self):
        return self._database
        
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
            oid: PgSchema(name, database)
            for oid, name in rows
        }

    @staticmethod
    def load(database, data):
        return PgSchema(
            data['name'],
            database
        )

    def filter_objects(self, database_filter):
        """
        Return new PgSchema object containing only filtered types and tables
        """
        schema = PgSchema(self.name, self)

        schema.types = list(
            filter(database_filter.include_type, self.types)
        )

        schema.tables = list(
            filter(database_filter.include_table, self.tables)
        )

        return schema

    def get_type(self, typename):
        for type in self.types + self.enum_types + self.composite_types + self.tables + self.views + self.aggregates:
            if type.name == typename:
                return type
        else:
            if self.name in SILENT_SCHEMAS or typename.endswith('[]'):
                return PgType(self, typename)
            else:
                raise KeyError('Type not defined in schema {}: {}'.format(self.name, typename))
            
    @property
    def objects(self):
        return self.types + self.enum_types + self.composite_types + self.tables + self.views + self.aggregates +\
            self.functions + self.sequences
            
    def get(self, name):
        for obj in self.objects:
            if obj.name == name:
                return obj
        else:
            return None

    def get_table(self, name):
        for table in self.tables:
            if table. name == name:
                return table
        else:
            return None

    def getall(self, name):
        return [obj for obj in self.objects if obj.name == name]

    def to_json(self):
        return OrderedDict([
            ('name', self.name),
        ])


class PgTable(PgObject):
    def __init__(self, schema, name, columns):
        self.schema = schema
        self.name = name
        self.columns = columns
        self.primary_key = None
        self.foreign_keys = []
        self.unique = None
        self.check = None
        self.description = None
        self.inherits = None
        self.indexes = []
        self.object_type = 'table'

    def __str__(self):
        return '"{}"."{}"'.format(self.schema.name, self.name)

    def get_dependencies(self):
        dependencies = [key.ref_table for key in self.foreign_keys]
        if self.inherits:
            dependencies.append(self.inherits)
        return dependencies
    
    @staticmethod
    def load_all_from_db(conn, database):
        query = (
            'SELECT pg_class.oid, relnamespace, relname, description '
            'FROM pg_class '
            'LEFT JOIN pg_description ON pg_description.objoid = pg_class.oid '
            'WHERE relkind = \'r\''
        )
        query_args = tuple()

        with closing(conn.cursor()) as cursor:
            cursor.execute(query, query_args)

            rows = cursor.fetchall()

        def table_from_row(row):
            oid, namespace_oid, name, description = row

            pg_table = PgTable(database.schemas[namespace_oid], name, [])

            if description is not None:
                pg_table.description = PgDescription(description)

            return pg_table

        tables = {
            row[0]: table_from_row(row)
            for row in rows
        }

        query = (
            'SELECT attrelid, attname, atttypid, attnotnull, atthasdef, '
            'pg_description.description, pg_attrdef.adbin, pg_attrdef.adsrc '
            'FROM pg_attribute '
            'LEFT JOIN pg_description '
            'ON pg_description.objoid = pg_attribute.attrelid '
            'AND pg_description.objsubid = pg_attribute.attnum '
            'LEFT JOIN pg_attrdef '
            'ON pg_attrdef.adrelid = pg_attribute.attrelid '
            'AND pg_attrdef.adnum = pg_attribute.attnum '
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
                    PgColumn.load(database, { 'name': column_name, 'data_type': database.types[column_type_oid],
                                              'nullable': not column_notnull, 'hasdef': column_hasdef,
                                              'default': column_default_human})
                    for table_oid, column_name, column_type_oid, column_notnull, column_hasdef, column_description,
                        column_default_binary, column_default_human in group
                ]

        query = (
            'SELECT inhrelid, inhparent FROM pg_inherits'
        )
        
        with closing(conn.cursor()) as cursor:
            cursor.execute(query)
            inheritance = cursor.fetchall()
            
        for (child_oid, parent_oid) in inheritance:
            if child_oid in tables and parent_oid in tables:
                tables[child_oid].inherits = tables[parent_oid]

        return tables

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

        description = data.get('description')

        if description is not None:
            table.description = PgDescription(description)

        primary_key_data = data.get('primary_key')

        if primary_key_data is not None:
            table.primary_key = PgPrimaryKey.load(primary_key_data)

        table.unique = data.get('unique')

        table.check = data.get('check')

        table.exclude = data.get('exclude')

        table.foreign_keys = [
            PgForeignKey.load(database, foreign_key)
            for foreign_key in data.get('foreign_keys', [])
        ]
        
        if 'inherits' in data:
            inherits_schema = database.schemas[data['inherits']['schema']]

            table.inherits = PgTableRef(inherits_schema, data['inherits']['name'])

        if 'indexes' in data:
            for index in data['indexes']:
                table.indexes.append(PgIndex.load(table, index))

        schema.tables.append(table)

        return table

    def to_json(self, short=False, showdefault=False):
        if short:
            if not showdefault and self.schema.name in SILENT_SCHEMAS:
                return self.name
            else:
                return '{}.{}'.format(self.schema.name, self.name)
        else:
            attributes = [
                ('name', self.name),
                ('schema', self.schema.name)
            ]

            if self.description is not None:
                attributes.append(('description', self.description))

            attributes.append(
                ('columns', [column.to_json() for column in self.columns])
            )

            if self.primary_key is not None:
                attributes.append(('primary_key', self.primary_key.to_json()))

            if self.inherits:
                attributes.append((
                    'inherits',
                    OrderedDict([
                        ('name', self.inherits.name),
                        ('schema', self.inherits.schema.name)
                    ])
                ))

            if self.foreign_keys:
                attributes.append((
                    'foreign_keys',
                    [foreign_key.to_json() for foreign_key in self.foreign_keys]
                ))

            if self.indexes:
                attributes.append((
                    'indexes',
                    [index.to_json() for index in self.indexes]
                ))

            return OrderedDict(attributes)


class PgPrimaryKey(PgObject):
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


class PgColumn(PgObject):
    def __init__(self, name, data_type):
        self.name = name
        self.data_type = data_type
        self.nullable = False
        self.description = None
        self.default = None

    def to_json(self):
        attributes = [
            ('name', self.name),
            ('data_type', self.data_type.to_json(short=True, showdefault=False)),
            ('nullable', self.nullable)
        ]

        if self.description is not None:
            attributes.append(('description', self.description))

        if self.default is not None:
            attributes.append(('default', self.default))
        return OrderedDict(attributes)

    @staticmethod
    def load(database, data):
        column = PgColumn(
            data['name'],
            database.get_type_ref(str(data['data_type']))
        )

        column.description = data.get('description')
        column.nullable = data.get('nullable', True)
        column.default = data.get('default', None)

        return column


class PgForeignKey:
    def __init__(self, schema, name, columns, ref_table, ref_columns, on_update = None, on_delete = None):
        self.schema = schema
        self.name = name
        self.columns = columns
        self.ref_table = ref_table
        self.ref_columns = ref_columns
        self.on_update = on_update
        self.on_delete = on_delete

    def get_name(self, obj):
        try:
            return obj.name
        except AttributeError:
            return obj
        
    def to_json(self):
        arguments = [
            ('name', self.name),
            ('columns', self.columns),
            ('references', OrderedDict([
                ('table', OrderedDict([
                    ('name', self.ref_table.name),
                    ('schema', self.ref_table.schema.name)
                ])),
                ('columns', self.ref_columns)
            ]))
        ]
        if self.on_update:
            arguments.append(('on_update', self.on_update))
        if self.on_delete:
            arguments.append(('on_delete', self.on_delete))
        return OrderedDict(arguments)

    @staticmethod
    def load_all_from_db(conn, database):
        action_type = {
            'a': None,
            'r': 'restrict',
            'c': 'cascade',
            'n': 'set null',
            'd': 'set default'
        }
    
        query = (
            'SELECT pg_constraint.oid, connamespace, conname, conrelid, '
            'array_agg(col.attname), confrelid, array_agg(refcol.attname), '
            'confupdtype, confdeltype '
            'FROM pg_constraint '
            'JOIN pg_attribute col '
            'ON col.attrelid = pg_constraint.conrelid '
            'AND col.attnum = ANY(conkey) '
            'JOIN pg_attribute refcol '
            'ON refcol.attrelid = pg_constraint.confrelid '
            'AND refcol.attnum = ANY(confkey) '
            'WHERE contype = \'f\' '
            'GROUP BY pg_constraint.oid, connamespace, conname, conrelid, '
            'confrelid, confupdtype, confdeltype'
        )

        with closing(conn.cursor()) as cursor:
            cursor.execute(query)

            rows = cursor.fetchall()

        def row_to_foreign_key(row):
            oid, namespace_oid, name, table_oid, columns, ref_table_oid, ref_columns, on_update, on_delete = row

            namespace = database.schemas[namespace_oid]

            table = database.tables[table_oid]

            ref_table = database.tables[ref_table_oid]

            pg_foreign_key = PgForeignKey(namespace, name, columns, ref_table, ref_columns, action_type[on_update], action_type[on_delete])

            table.foreign_keys.append(pg_foreign_key)

            return oid, pg_foreign_key
        
        return dict(row_to_foreign_key(row) for row in rows)

    @staticmethod
    def load(database, data):
        return PgForeignKey(
            data['references']['table']['schema'],
            data['name'],
            data['columns'],
            data['references']['table']['name'],
            data['references']['columns'],
            data.get('on_update', None),
            data.get('on_delete', None)
        )


class PgEnumType(PgObject):
    def __init__(self, schema, name, labels):
        self.schema = schema
        self.name = name
        self.labels = labels
        self.object_type = 'enum_type'

    @staticmethod
    def load(database, data):
        schema = database.register_schema(data['schema'])

        return PgEnumType(schema, data['name'], data['labels'])

    def to_json(self):
        return OrderedDict([
            ('schema', self.schema.name),
            ('name', self.name),
            ('labels', self.labels)
        ])

    @staticmethod
    def load_all_from_db(conn, database):
        query = (
            'SELECT pg_type.oid, pg_type.typnamespace, pg_type.typname, '
            'array_agg(enumlabel) '
            'FROM pg_type '
            'JOIN pg_enum ON pg_type.oid = pg_enum.enumtypid '
            'WHERE typtype = \'e\''
            'GROUP BY pg_type.oid, pg_type.typnamespace, pg_type.typname'
        )
        query_args = tuple()

        with closing(conn.cursor()) as cursor:
            cursor.execute(query, query_args)

            rows = cursor.fetchall()

        return {
            oid: PgEnumType(database.schemas[namespace_oid], name, labels)
            for oid, namespace_oid, name, labels in rows
        }


class PgFunction(PgObject):
    volatilities = {
        "v": "volatile",
        "s": "stable",
        "i": "immutable"
        }
    
    def __init__(self, schema, name, arguments, return_type):
        self.schema = schema
        self.name = name
        self.arguments = arguments
        self.return_type = return_type
        self.returns_set = False
        self.src = None
        self.language = None
        self.description = None
        self.volatility = 'volatile'
        self.strict = False
        self.object_type = 'function'

    def __str__(self):
        return '"{}"."{}"'.format(self.schema.name, self.name)

    def get_dependencies(self):
        return [argument.data_type for argument in self.arguments] + [self.return_type] +\
               self.database.find_dependencies(self.src)
        
    @staticmethod
    def load(database, data):
        schema = database.register_schema(data['schema'])
        
        pg_function = PgFunction(
            schema,
            data['name'],
            [PgArgument.from_json(argument) for argument in data['arguments']],
            database.get_type_ref(str(data['return_type']))
        )

        pg_function.language = data.get('language')
        pg_function.src = PgSourceCode(data['source'])
        pg_function.description = data.get('description')
        pg_function.returns_set = data.get('returns_set', False)
        pg_function.volatility = data.get('volatility', 'volatile')
        pg_function.strict = data.get('strict', False)

        schema.functions.append(pg_function)

        return pg_function

    def ident(self):
        return '{}.{}'.format(self.schema.name, self.name)

    def to_json(self):
        attributes = [
            ('name', self.name),
            ('schema', self.schema.name),
            ('return_type', self.return_type.to_json(short=True, showdefault=False))
        ]

        if self.returns_set:
            attributes.append(('returns_set', self.returns_set))

        attributes.extend([
            ('language', self.language),
            ('volatility', self.volatility),
            ('strict', self.strict),
            ('arguments', [
                argument.to_json()
                for argument
                in self.arguments
            ])
        ])

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
            'pg_language.lanname, proretset, prosrc, provolatile, '
            'proisstrict, description '
            'FROM pg_proc '
            'JOIN pg_language ON pg_language.oid = pg_proc.prolang '
            'LEFT JOIN pg_description ON pg_description.objoid = pg_proc.oid '
            'WHERE proisagg IS false'
        )

        query_args = tuple()

        with closing(conn.cursor()) as cursor:
            cursor.execute(query, query_args)

            rows = cursor.fetchall()

        def function_from_row(row):
            (
                oid, namespace_oid, name, return_type_oid, arg_type_oids_str,
                all_arg_type_oids, arg_modes, arg_names, language, returns_set,
                src, volatility, strict, description
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
                PgArgument(empty_str_filter(name), database.types[type_oid], arg_mode, None)
                for type_oid, name, arg_mode in zip(all_arg_type_oids, arg_names, arg_modes)
            ]

            pg_function = PgFunction(
                database.schemas[namespace_oid], name, arguments,
                database.types[return_type_oid]
            )
            pg_function.language = language
            pg_function.src = PgSourceCode(src.strip())
            pg_function.returns_set = returns_set
            try:
                pg_function.volatility = PgFunction.volatilities[volatility]
            except KeyError:
                pg_function.volatility = 'volatile'
            pg_function.strict = strict

            if description is not None:
                pg_function.description = PgDescription(description)

            return pg_function

        return {
            row[0]: function_from_row(row)
            for row in rows
        }


class PgTrigger(PgObject):
    def __init__(self, table, name, function, tgtype=None):
        self.table = table
        self.name = name
        self.function = function
        if tgtype:
            (self.timing, self.triggertypes, self.affecteach) = self.analyze_type(tgtype)
        self.object_type = 'trigger'

    def __str__(self):
        return '{}.{}'.format(str(self.table), self.name)
        
    def get_dependencies(self):
        return [self.table, self.function]

    @property
    def schema(self):
        return self.table.schema

    def analyze_type(self, tgtype):
        triggertypes = []
        affect_row = tgtype % 2
        tgtype //= 2
        timing = 'before' if tgtype % 2 else 'after'
        tgtype //= 2
        if tgtype % 2:
            triggertypes.append('insert')
        tgtype //= 2
        if tgtype % 2:
            triggertypes.append('delete')
        tgtype //= 2
        if tgtype % 2:
            triggertypes.append('update')
        tgtype //= 2
        if tgtype % 2:
            triggertypes.append('truncate')
        tgtype //= 2
        if tgtype % 2:
            timing = 'instead'
        return (timing, triggertypes, 'row' if affect_row else 'statement')

    @staticmethod
    def load_all_from_db(conn, database):
        query = (
            'SELECT oid, tgrelid, tgname, tgfoid, tgtype '
            'from pg_trigger where NOT tgisinternal;'
            )

        with closing(conn.cursor()) as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()

        def trigger_from_row(row):
            oid, tableid, name, functionid, tgtype = row
            return PgTrigger(database.tables[tableid], name, database.functions[functionid], tgtype)

        return {
            row[0]: trigger_from_row(row)
            for row in rows
        }
    
    @staticmethod
    def load(database, data):
        pg_trigger = PgTrigger(
            database.get_schema_by_name(data['table']['schema']).get(data['table']['name']),
            data['name'],
            database.get_schema_by_name(data['function']['schema']).get(data['function']['name'])
        )

        pg_trigger.timing = data.get('timing', 'after')
        pg_trigger.triggertypes = data['triggertypes']
        pg_trigger.affecteach = data.get('affecteach', 'statement')

        return pg_trigger

    def to_json(self):
        attributes = [
            ('table', OrderedDict([
                ('schema', self.table.schema.name),
                ('name', self.table.name)
            ])),
            ('name', self.name),
            ('function', OrderedDict([
                ('schema', self.function.schema.name),
                ('name', self.function.name)
            ])),
            ('timing', self.timing),
            ('triggertypes', self.triggertypes),
            ('affecteach', self.affecteach)
            ]
        return OrderedDict(attributes)


class PgCast(PgObject):
    def __init__(self, source, target, function, implicit = False):
        self.source = source
        self.target = target
        self.function = function
        self.implicit = implicit
        self.object_type = 'cast'

    def __str__(self):
        return '{}::{}'.format(str(self.source), str(self.target))

    def get_dependencies(self):
        return [self.source, self.target, self.function]

    @property
    def schema(self):
        if self.source.schema.name in SKIPPED_SCHEMAS:
            return self.target.schema
        else:
            return self.source.schema

    @staticmethod
    def load_all_from_db(conn, database):
        query = (
            'SELECT oid, castsource, casttarget, castfunc, castcontext '
            'FROM pg_cast '
            'WHERE castmethod = \'f\''
            )

        with closing(conn.cursor()) as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
            
        def cast_from_row(row):
            oid, sourceid, targetid, funcit, context = row
            return PgCast(
                database.types[sourceid],
                database.types[targetid],
                database.functions[funcit],
                context == 'i'
            )

        return {
            row[0]: cast_from_row(row)
            for row in rows
        }

    @staticmethod
    def load(database, data):
        return PgCast(
            database.get_schema_by_name(data['source']['schema']).get_type(data['source']['name']),
            database.get_schema_by_name(data['target']['schema']).get_type(data['target']['name']),
            database.get_schema_by_name(data['function']['schema']).get(data['function']['name']),
            data.get('implicit', False)
        )

    def to_json(self):
        attributes = [
            ('source', OrderedDict([
                ('schema', self.source.schema.name),
                ('name', self.source.name)
            ])),
            ('target', OrderedDict([
                ('schema', self.target.schema.name),
                ('name', self.target.name)
            ])),
            ('function', OrderedDict([
                ('schema', self.function.schema.name),
                ('name', self.function.name)
            ])),
            ('implicit', self.implicit)
            ]
        return OrderedDict(attributes)
    
    
class PgSequence(PgObject):
    def __init__(self, schema, name, startvalue="1", minvalue=None, maxvalue=None, increment="1"):
        self.schema = schema
        self.name = name
        self.start_value = startvalue
        self.minimum_value = minvalue
        self.maximum_value = maxvalue
        self.increment = increment
        self.object_type = 'sequence'
        
    @staticmethod
    def load(database, data):
        schema = database.register_schema(data['schema'])
        
        pg_sequence = PgSequence(
            schema,
            data['name']
        )

        pg_sequence.start_value = data.get('startvalue', "1")
        pg_sequence.minimum_value = data.get('minimumvalue')
        pg_sequence.maximum_value = data.get('maximumvalue')
        pg_sequence.increment = data.get('increment', "1")

        schema.sequences.append(pg_sequence)

        return pg_sequence

    def ident(self):
        return '{}.{}'.format(self.schema.name, self.name)

    def to_json(self):
        attributes = [
            ('name', self.name),
            ('schema', self.schema.name),
            ('startvalue', self.start_value)
        ]
        if self.minimum_value:
            attributes.append(('minimumvalue', self.minimum_value))
        if self.maximum_value:
            attributes.append(('maximumvalue', self.maximum_value))
        attributes.append(('increment', self.increment))

        return OrderedDict(attributes)

    @staticmethod
    def load_all_from_db(conn, database):
        query = (
            'SELECT sequence_schema, sequence_name, start_value, minimum_value, maximum_value, increment '
            'FROM information_schema.sequences'
        )

        with closing(conn.cursor()) as cursor:
            cursor.execute(query)

            rows = cursor.fetchall()

        def sequence_from_row(row):
            (schema, name, start_value, minimum_value, maximum_value, increment) = row
            minimum_value = str(minimum_value)
            maximum_value = str(maximum_value)
            if minimum_value == "1":
                minimum_value = None
            if maximum_value in ["2147483647",  "9223372036854775807"]:
                maximum_value = None

            return PgSequence(
                database.get_schema_by_name(schema), name, start_value, minimum_value, maximum_value, increment
            )

        return {
            "{}.{}".format(row[0], row[1]): sequence_from_row(row)
            for row in rows
        }
    
    
class PgAggregate(PgObject):
    def __init__(self, schema, name, arguments):
        self.schema = schema
        self.name = name
        self.arguments = arguments
        self.sfunc = None
        self.stype = None
        self.object_type = 'aggregate'

    def ident(self):
        return '{}.{}'.format(self.schema.name, self.name)

    def get_dependencies(self):
        return [argument.data_type for argument in self.arguments] + [self.sfunc.dereference()] + [self.stype.dereference()]

    @staticmethod
    def load(database, data):
        schema = database.register_schema(data['schema'])

        arguments = [PgArgument.from_json(argument) for argument in data['arguments']]

        aggregate = PgAggregate(schema, data['name'], arguments)

        aggregate.sfunc = PgFunctionRef(None, data['sfunc'])
        aggregate.stype = PgTypeRef(None, data['stype'])

        schema.aggregates.append(aggregate)

        return aggregate

    def to_json(self):
        attributes = [
            ('name', self.name),
            ('schema', self.schema.name),
            ('sfunc', self.sfunc.ident()),
            ('stype', self.stype.ident()),
            ('arguments', [
                argument.to_json() for argument in self.arguments
            ])
        ]

        return OrderedDict(attributes)

    @staticmethod
    def load_all_from_db(conn, database):
        query = (
            'SELECT pg_proc.oid, pronamespace, proname, aggtransfn::oid, '
            'aggtranstype, proargtypes, proallargtypes, proargmodes, '
            'proargnames, description '
            'FROM pg_proc '
            'JOIN pg_aggregate ON pg_aggregate.aggfnoid = pg_proc.oid '
            'LEFT JOIN pg_description ON pg_description.objoid = pg_proc.oid'
        )

        query_args = tuple()

        with closing(conn.cursor()) as cursor:
            cursor.execute(query, query_args) 

            rows = cursor.fetchall()

        def aggregate_from_row(row):
            (
                oid, namespace_oid, name, sfunc_oid, stype_oid,
                arg_type_oids_str, all_arg_type_oids, arg_modes, arg_names,
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
                PgArgument(empty_str_filter(name), database.types[type_oid], arg_mode, None)
                for type_oid, name, arg_mode in zip(all_arg_type_oids, arg_names, arg_modes)
            ]

            aggregate = PgAggregate(database.schemas[namespace_oid], name, arguments)
            aggregate.sfunc = database.functions[sfunc_oid]
            aggregate.stype = database.types[stype_oid]

            return aggregate

        return {
            row[0]: aggregate_from_row(row)
            for row in rows
        }


class PgRole(PgObject):
    known_roles = {} # if necessary contains names as keys, roles as values
    
    def __init__(self, name, superuser, inherit, createrole, createdb, login, membership = None):
        self.name = name
        self.super = superuser
        self.inherit = inherit
        self.createrole = createrole
        self.createdb = createdb
        self.login = login
        self.membership = membership or []
        self.schema = self # a hack because roles don't have a schema, but some functions assume presence
        self.object_type = 'role'

    def ident(self):
        return self.name

    def get_dependencies(self):
        return self.membership

    @staticmethod
    def load(database, data):
        roles = [PgRole.known_roles[membership] for membership in data.get('memberships', [])]
        pg_role = PgRole(
            data['name'], data.get('super', False), data.get('inherit', True),
            data.get('createrole', False), data.get('createdb', False),
            data.get('login', False), roles
        )
        
        PgRole.known_roles[data['name']] = pg_role

        return pg_role

    def to_json(self):
        attributes = [
            ('name', self.name),
            ('super', self.super),
            ('inherit', self.inherit),
            ('createrole', self.createrole),
            ('createdb', self.createdb),
            ('login', self.login)
            ]
        if self.membership:
            attributes.append(('memberships', [role.name for role in self.membership]))
            
        return OrderedDict(attributes)

    def load_all_from_db(conn, database):
        query = (
            "SELECT oid, rolname, rolsuper, rolinherit, rolcreaterole, rolcreatedb, rolcanlogin "
            "FROM pg_roles "
            "WHERE rolname <> 'postgres' AND rolname NOT LIKE 'pg_%'"
            )

        with closing(conn.cursor()) as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()

        def role_from_row(row):
            (oid, name, superuser, inherit, createrole, createdb, canlogin) = row
            return PgRole(name, superuser, inherit, createrole, createdb, canlogin)
            
        roles = {
            row[0]: role_from_row(row)
            for row in rows
        }

        query = (
            "SELECT roleid, member FROM pg_auth_members"
            )

        with closing(conn.cursor()) as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()

        for row in rows:
            (role, member) = row
            if role in roles and member in roles:
                roles[member].membership.append(roles[role])

        return roles
            
    
data_type_mapping = {
    'int2': 'smallint',
    'int4': 'integer',
    'int8': 'bigint'
}


class PgTypeRef(PgObject):
    def __init__(self, registry, ref):
        self.registry = registry
        self.ref = ref

    def __str__(self):
        if self.registry.name in SILENT_SCHEMAS:
            return self.ref
        else:
            return '{}.{}'.format(self.registry.name, self.ref)

    def ident(self):
        return self.ref
    
    def dereference(self):
        try:
            return self.registry.get_type(self.ref)
        except AttributeError:
            return self.registry.get(self.ref)

    def to_json(self, short=False, showdefault=True):
        try:
            return self.dereference().to_json(short=short, showdefault=showdefault)
        except (AttributeError, KeyError):
            if not self.registry:
                return self.ref
            elif not showdefault and self.registry.name in SILENT_SCHEMAS:
                return self.ref
            else:
                return '{}.{}'.format(self.registry.name, self.ref)

    @property
    def object_type(self):
        try:
            return self.dereference().object_type
        except AttributeError:
            return 'type'


class PgFunctionRef(PgObject):
    def __init__(self, registry, ref):
        self.registry = registry
        self.ref = ref
        self.object_type = 'function'

    def ident(self):
        return self.ref

    def dereference(self):
        return self.registry.get(self.ref)


class PgTableRef(PgObject):
    def __init__(self, registry, ref):
        self.registry = registry
        self.ref = ref
        self.schema = registry
        self.name = ref
        self.object_type = 'table'

    def __str__(self):
        return '"{}"."{}"'.format(self.registry.name, self.ref)

    def dereference(self):
        return self.registry.get(self.ref)

    def to_json(self, short=False, showdefault=False):
        return self.dereference().to_json(short=short, showdefault=showdefault)
    
    
class PgType(PgObject):
    def __init__(self, schema, name):
        self.schema = schema
        self.name = name
        self.element_type = None
        self.object_type = 'type'

    def get_dependencies(self):
        return [self.element_type] if self.element_type else []

    @staticmethod
    def load_all_from_db(conn, database):
        query = (
            "SELECT oid, typname, typnamespace, typelem, typcategory "
            "FROM pg_type"
        )

        with closing(conn.cursor()) as cursor:
            cursor.execute(query)

            rows = cursor.fetchall()

        pg_types = {}

        for oid, name, namespace_oid, element_oid, category in rows:
            pg_type = PgType(database.schemas[namespace_oid], name)

            if category == 'A' and element_oid != 0:
                # Store a reference, because the targeted type may not be
                # loaded yet
                pg_type.element_type = PgTypeRef(pg_types, element_oid)

            pg_types[oid] = pg_type

        # Dereference all references
        for pg_type in pg_types.values():
            if pg_type.element_type is not None:
                pg_type.element_type = pg_type.element_type.dereference()

        return pg_types

    def ident(self):
        return str(self)

    def to_json(self, short=False, showdefault=True):
        if not showdefault and self.schema.name in SILENT_SCHEMAS:
            return self.name
        else:
            return str(self)

    def __str__(self):
        if self.schema is None or self.schema.name == 'pg_catalog':
            return data_type_mapping.get(self.name, "{}[]".format(str(self.element_type)) if self.element_type else self.name)
        else:
            if self.element_type is not None:
                return "{}[]".format(str(self.element_type))
            else:
                if self.schema is None:
                    return self.name
                else:
                    return '{}.{}'.format(self.schema.name, self.name)


class PgCompositeType(PgObject):
    def __init__(self, schema, name, columns):
        self.schema = schema
        self.name = name
        self.columns = columns
        self.object_type = 'composite_type'

    def __str__(self):
        if self.schema in SILENT_SCHEMAS:
            return self.name
        else:
            return '{}.{}'.format(self.schema.name, self.name)

    def get_dependencies(self):
        return [column.data_type for column in self.columns]

    @staticmethod
    def load(database, data):
        schema = database.register_schema(data.get('schema', DEFAULT_SCHEMA))

        composite_type = PgCompositeType(
            schema,
            data['name'],
            [
                PgColumn.load(database, column_data)
                for column_data in data['columns']
            ]
        )

        schema.composite_types.append(composite_type)
        
        return composite_type

    def to_json(self, short=False, showdefault=True):
        if short:
            if not showdefault and self.schema.name in SILENT_SCHEMAS:
                return self.name
            else:
                return '{}.{}'.format(self.schema.name, self.name)
        else:
            attributes = [
                ('name', self.name),
                ('schema', self.schema.name),
                ('columns', [column.to_json() for column in self.columns])
            ]

            return OrderedDict(attributes)

    @staticmethod
    def load_all_from_db(conn, database):
        query = (
            'SELECT pg_type.typrelid, pg_type.typnamespace, pg_type.typname '
            'FROM pg_type '
            'JOIN pg_class ON pg_type.typrelid = pg_class.oid '
            'WHERE relkind = \'c\''
        )
        query_args = tuple()

        with closing(conn.cursor()) as cursor:
            cursor.execute(query, query_args)

            rows = cursor.fetchall()

        composite_types = {
            rel_oid: PgCompositeType(database.schemas[namespace_oid], name, [])
            for rel_oid, namespace_oid, name in rows
        }

        query = (
            'SELECT attrelid, attname, atttypid, pg_description.description '
            'FROM pg_attribute '
            'JOIN pg_class ON pg_class.oid = pg_attribute.attrelid '
            'LEFT JOIN pg_description '
            'ON pg_description.objoid = pg_attribute.attrelid '
            'AND pg_description.objsubid = pg_attribute.attnum '
            'WHERE pg_class.relkind = \'c\' AND attnum > 0'
        )

        query_args = tuple()

        with closing(conn.cursor()) as cursor:
            cursor.execute(query, query_args)

            column_rows = cursor.fetchall()

        sorted_column_rows = sorted(column_rows, key=itemgetter(0))

        for key, group in itertools.groupby(sorted_column_rows, key=itemgetter(0)):
            table = composite_types.get(key)

            if table is not None:
                table.columns = [
                    PgColumn(column_name, database.types[column_type_oid])
                    for table_oid, column_name, column_type_oid, column_description in group
                ]

        return composite_types


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
            PgTypeRef(None, data['data_type']),
            data.get('mode', 'i'),
            data.get('default')
        )

    def to_json(self):
        attributes = []

        if self.name is not None:
            attributes.append(('name', self.name))

        attributes.append(('data_type', self.data_type.to_json(short=True, showdefault=False)))

        if self.mode is not None and self.mode != 'i':
            attributes.append(('mode', self.mode))

        if self.default is not None:
            attributes.append(('default', self.default))

        return OrderedDict(attributes)


class PgViewQuery(str):
    pass


class PgView(PgObject):
    def __init__(self, schema, name, view_query):
        self.schema = schema
        self.name = name
        self.view_query = view_query
        self.object_type = 'view'

    def get_dependencies(self):
        return self.database.find_dependencies(self.view_query)

    @staticmethod
    def load(database, data):
        schema = database.register_schema(data['schema'])

        pg_view = PgView(
            schema,
            data['name'],
            PgViewQuery(data['query'])
        )

        schema.views.append(pg_view)

        return pg_view

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
            oid: PgView(
                database.schemas[namespace_oid], name, PgViewQuery(view_def)
            )
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


class PgSetting(PgObject):
    def __init__(self, name, value):
        self.name = name
        self.value = value

    @staticmethod
    def load_all_from_db(conn, database):
        raise NotImplementedError

    @staticmethod
    def load(database, data):
        return PgSetting(data['name'], data['value'])
    
    def to_json(self):
        attributes = [
            ('name', self.name),
            ('value', self.value)
            ]
        return OrderedDict(attributes)


class PgRow(PgObject):
    def __init__(self, table):
        self.table = table
        self.values = OrderedDict()
        self.schema = table.schema

    def get_dependencies(self):
        return [self.table]

    @staticmethod
    def load_all_from_db(conn, database):
        raise NotImplementedError

    @staticmethod
    def load(database, data):
        pgrow = PgRow(database.get_schema_by_name(data['table']['schema']).get(data['table']['name']))
        for value in data['values']:
            pgrow.values[value['column']] = str(value['value']).lower()
            if value.get('string', False):
                pgrow.values[value['column']] = "'{}'".format(pgrow.values[value['column']])
        return pgrow

    def to_json(self):
        # WARNING: This function has not been tested yet!!!
        values = []
        for column in self.values:
            if values[column].startswith("'") and values[column].endswith("'"):
                values.append(OrderedDict([
                    ('column', column),
                    ('value', values[column][1:-1])
                    ('string', True)
                ]))
            else:
                values.append(OrderedDict([
                    ('column', column),
                    ('value', values[column])
                ]))
        attributes = [
            ('table', OrderedDict([
                ('name', self.table.name),
                ('schema', self.table.schema.name)
            ]))
            ('values', values)
            ]
        return OrderedDict(attributes)


class PgIndex:
    def __init__(self, table, name, definition):
        self.table = table
        self.name = name
        self.definition = definition
        self.schema = table.schema

    @staticmethod
    def load_all_from_db(conn, database):
        query = (
            "SELECT schemaname, tablename, indexname, indexdef "
            "FROM pg_indexes"
        )

        with closing(conn.cursor()) as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()

        for (schemaname, tablename, name, definition) in rows:
            if name.endswith('_pkey'):
                continue
            table = database.get_schema_by_name(schemaname).get_table(tablename)
            table.indexes.append(PgIndex(table, name, definition))

        return OrderedDict()

    @staticmethod
    def load(table, data):
        return PgIndex(table, data['name'], data['definition'])

    def to_json(self):
        return OrderedDict([
            ('name', self.name),
            ('definition', self.definition)
        ])
    
    
class PgDepend:
    def __init__(self, dependent_obj, referenced_obj):
        self.dependent_obj = dependent_obj
        self.referenced_obj = referenced_obj

    @staticmethod
    def load_all_from_db(conn, database):
        query = (
            "SELECT classid, objid, objsubid, refclassid, refobjid, "
            "refobjsubid, deptype "
            "FROM pg_depend"
        )

        with closing(conn.cursor()) as cursor:
            cursor.execute(query)

            rows = cursor.fetchall()

        def get_object(classid, objid, objsubid):
            if classid == 0:
                return None

            if database.tables[classid].name == 'pg_type':
                return database.types[objid]

        def row_to_pg_depend(row):
            (
                classid, objid, objsubid, refclassid, refobjid, refobjsubid,
                deptype
            ) = row

            dependent_obj = get_object(classid, objid, objsubid)
            referenced_obj = get_object(refclassid, refobjid, refobjsubid)

            return PgDepend(dependent_obj, referenced_obj)

        return [row_to_pg_depend(row) for row in rows]


object_loaders = {
    'schema': PgSchema.load,
    'table': PgTable.load,
    'function': PgFunction.load,
    'view': PgView.load,
    'composite_type': PgCompositeType.load,
    'enum_type': PgEnumType.load,
    'aggregate': PgAggregate.load,
    'sequence': PgSequence.load,
    'role': PgRole.load,
    'trigger': PgTrigger.load,
    'cast': PgCast.load,
    'setting': PgSetting.load,
    'row': PgRow.load
}


def load_object(database, object_data):
    object_type, object_data = next(iter(object_data.items()))
    
    try:
        return object_loaders[object_type](database, object_data)
    except IndexError:
        raise Exception('Unsupported object type: {}'.format(object_type))


def empty_str_filter(maybe_empty_str):
    if maybe_empty_str is None:
        return None
    else:
        if len(maybe_empty_str) == 0:
            return None
        else:
            return maybe_empty_str
