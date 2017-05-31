from itertools import chain

from pg_db_tools import iter_join
from pg_db_tools.pg_types import PgEnum


class SqlRenderer:
    def __init__(self):
        self.if_not_exists = False

    def render(self, out_file, database):
        rendered_chunks = self.render_chunks(database)

        out_file.writelines(rendered_chunks)

    def render_chunks(self, database):
        return iter_join(
            '\n',
            chain(*self.render_chunk_sets(database))
        )

    def render_chunk_sets(self, database):
        yield self.create_extension_statements(database)

        for schema in sorted(
                database.schemas.values(), key=lambda s: s.name):
            for sql in self.render_schema_sql(schema):
                yield sql

        for schema in sorted(database.schemas.values(), key=lambda s: s.name):
            for table in schema.tables:
                for index, foreign_key in enumerate(table.foreign_keys):
                    yield SqlRenderer.render_foreign_key(
                        index, schema, table, foreign_key
                    )

    @staticmethod
    def render_foreign_key(index, schema, table, foreign_key):
        return [(
            'ALTER TABLE {schema_name}.{table_name} '
            'ADD CONSTRAINT {key_name} '
            'FOREIGN KEY ({columns}) '
            'REFERENCES {ref_schema_name}.{ref_table_name} ({ref_columns});'
        ).format(
            schema_name=quote_ident(schema.name),
            table_name=quote_ident(table.name),
            key_name=quote_ident(
                '{}_{}_fk_{}'.format(schema.name, table.name, index)
            ),
            columns=', '.join(foreign_key['columns']),
            ref_schema_name=quote_ident(
                foreign_key['references']['table']['schema']
            ),
            ref_table_name=quote_ident(
                foreign_key['references']['table']['name']
            ),
            ref_columns=', '.join(foreign_key['references']['columns'])
        )]

    def render_schema_sql(self, schema):
        # Assume the public schema already exists
        if schema.name != 'public':
            yield [self.create_schema_statement(schema)]

        for type_data in schema.types:
            yield self.render_type_sql(type_data)

        for table in schema.tables:
            yield self.render_table_sql(table)

    def create_schema_statement(self, schema):
        options = []

        if self.if_not_exists:
            options.append('IF NOT EXISTS')

        return 'CREATE SCHEMA {options}{ident};\n'.format(
            options=''.join('{} '.format(option) for option in options),
            ident=quote_ident(schema.name)
        )

    def create_extension_statements(self, database):
        options = []

        if self.if_not_exists:
            options.append('IF NOT EXISTS')

        for extension_name in database.extensions:
            yield 'CREATE EXTENSION {options}{extension_name};\n'.format(
                options=''.join('{} '.format(option) for option in options),
                extension_name=extension_name
            )

    def render_type_sql(self, data):
        type_type = type(data)

        if type_type is PgEnum:
            return self.render_enum_sql(data)
        else:
            raise Exception('Unsupported type: {}'.format(type_type))

    def render_enum_sql(self, enum):
        return [
            'CREATE TYPE {ident} AS ENUM ({values});\n'.format(
                ident='{}.{}'.format(
                    quote_ident(enum.schema.name), quote_ident(enum.name)
                ),
                values=', '.join(map(quote_string, enum.values))
            )
        ]

    def render_table_sql(self, table):
        options = []

        if self.if_not_exists:
            options.append('IF NOT EXISTS')

        yield (
            'CREATE TABLE {options}{ident}\n'
            '(\n'
            '{columns_part}\n'
            ');\n'
        ).format(
            options=''.join('{} '.format(option) for option in options),
            ident='{}.{}'.format(
                quote_ident(table.schema.name), quote_ident(table.name)
            ),
            columns_part=',\n'.join(self.table_defining_components(table))
        )

        if table.description:
            yield (
                'COMMENT ON TABLE {} IS {};\n'
            ).format(
                '{}.{}'.format(
                    quote_ident(table.schema), quote_ident(table.name)
                ),
                quote_string(escape_string(table.description))
            )

    def table_defining_components(self, table):
        for column_data in table.columns:
            yield '  {}'.format(self.render_column_definition(column_data))

        if table.primary_key:
            yield '  PRIMARY KEY ({})'.format(', '.join(table.primary_key))

        if table.unique:
            for unique_constraint in table.unique:
                yield '  UNIQUE ({})'.format(
                    ', '.join(unique_constraint['columns'])
                )

        if table.check:
            for check_constraint in table.check:
                yield '  CHECK ({})'.format(check_constraint['expression'])

        if table.exclude:
            for exclude_constraint in table.exclude:
                yield '  {}'.format(
                    self.render_exclude_constraint(exclude_constraint)
                )

    def render_column_definition(self, column):
        parts = [
            quote_ident(column.name),
            column.data_type
        ]

        if column.nullable is False:
            parts.append('NOT NULL')

        if column.default:
            parts.append('DEFAULT {}'.format(column.default))

        return ' '.join(parts)

    def render_exclude_constraint(self, exclude_data):
        parts = ['EXCLUDE ']

        if exclude_data.get('index_method'):
            parts.append('USING {index_method} '.format(**exclude_data))

        parts.append(
            '({})'.format(
                ', '.join(
                    '{exclude_element} WITH {operator}'.format(**e)
                    for e in exclude_data['exclusions']
                )
            )
        )

        return ''.join(parts)


def quote_ident(ident):
    return '"' + ident + '"'


def quote_string(string):
    return "'" + string + "'"


def escape_string(string):
    return string.replace("'", "''")
