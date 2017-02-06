import yaml


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


def load(infile):
    data = yaml.load(infile)

    database = PgDatabase()

    database.extensions = data.get('extensions', [])

    types = [load_type(database, type_data) for type_data in data['types']]

    objects = [load_object(database, object_data) for object_data in data['objects']]

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
        self.description = None

    @staticmethod
    def load(database, data):
        schema = database.register_schema(data['schema'])

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

        table.exclude = data.get('exclude')

        table.foreign_keys = [foreign_key for foreign_key in data.get('foreign_keys', [])]

        schema.tables.append(table)

        return table


class PgColumn:
    def __init__(self, name, data_type):
        self.name = name
        self.data_type = data_type
        self.nullable = False

    @staticmethod
    def load(database, data):
        column = PgColumn(
            data['name'],
            data['data_type']
        )

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
