from itertools import chain

from pg_db_tools import iter_join
from pg_db_tools.graph import database_to_graph
from pg_db_tools.pg_types import PgEnumType, PgTable, PgFunction, PgView, \
    PgCompositeType, PgAggregate


def render_table_sql(table):
    options = []
    post_options = []

    if table.inherits:
        post_options.append('INHERITS ({}.{})'.format(
            quote_ident(table.inherits.schema.name),
            quote_ident(table.inherits.name)
        ))

    yield (
        'CREATE TABLE {options}{ident}\n'
        '(\n'
        '{columns_part}\n'
        '){post_options};\n'
    ).format(
        options=''.join('{} '.format(option) for option in options),
        ident='{}.{}'.format(
            quote_ident(table.schema.name), quote_ident(table.name)
        ),
        columns_part=',\n'.join(table_defining_components(table)),
        post_options=' '.join(post_options)
    )

    if table.description:
        yield (
            'COMMENT ON TABLE {} IS {};\n'
        ).format(
            '{}.{}'.format(
                quote_ident(table.schema.name), quote_ident(table.name)
            ),
            quote_string(escape_string(table.description))
        )


def table_defining_components(table):
    for column_data in table.columns:
        yield '  {}'.format(render_column_definition(column_data))

    if table.primary_key:
        yield '  PRIMARY KEY ({})'.format(', '.join(table.primary_key.columns))

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
                render_exclude_constraint(exclude_constraint)
            )


def render_column_definition(column):
    parts = [
        quote_ident(column.name),
        column.data_type
    ]

    if column.nullable is False:
        parts.append('NOT NULL')

    if column.default:
        parts.append('DEFAULT {}'.format(column.default))

    return ' '.join(parts)


def render_composite_type_column_definition(column):
    return '{} {}'.format(quote_ident(column.name), column.data_type)


def render_exclude_constraint(exclude_data):
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


def render_function_sql(pg_function):
    returns_part = '    RETURNS '

    table_arguments = [
        argument for argument in pg_function.arguments if argument.mode == 't'
    ]

    if table_arguments:
        returns_part += 'TABLE({})'.format(', '.join(render_argument(argument) for argument in table_arguments))
    else:
        if pg_function.returns_set:
            returns_part += 'SETOF '

        returns_part += pg_function.return_type

    return [
        'CREATE FUNCTION "{}"."{}"({})'.format(
            pg_function.schema.name, pg_function.name,
            ', '.join(render_argument(argument) for argument in pg_function.arguments if argument.mode in ('i', 'o', 'b', 'v'))
        ),
        returns_part,
        'AS $$',
        str(pg_function.src),
        '$$ LANGUAGE {};'.format(pg_function.language)
    ]


def render_view_sql(pg_view):
    return [
        'CREATE VIEW "{}"."{}" AS'.format(pg_view.schema.name, pg_view.name),
        pg_view.view_query
    ]


def render_composite_type_sql(pg_composite_type):
    yield (
        'CREATE TYPE {ident} AS (\n'
        '{columns_part}\n'
        ');\n'
    ).format(
        ident='{}.{}'.format(quote_ident(pg_composite_type.schema.name), quote_ident(pg_composite_type.name)),
        columns_part=',\n'.join(
            '  {}'.format(render_composite_type_column_definition(column_data))
            for column_data in pg_composite_type.columns
        )
    )


def render_enum_type_sql(pg_enum_type):
    yield (
        'CREATE TYPE {ident} AS ENUM (\n'
        '{labels_part}\n'
        ');\n'
    ).format(
        ident='{}.{}'.format(quote_ident(pg_enum_type.schema.name), quote_ident(pg_enum_type.name)),
        labels_part=',\n'.join('  {}'.format(quote_string(label)) for label in pg_enum_type.labels)
    )


def render_aggregate_sql(pg_aggregate):
    properties = [
        '    SFUNC = {}'.format(pg_aggregate.sfunc.ident()),
        '    STYPE = {}'.format(pg_aggregate.stype.ident())
    ]

    yield (
        'CREATE AGGREGATE {ident} ({arguments}) (\n'
        '{properties}\n'
        ');\n'
    ).format(
        ident=pg_aggregate.ident(),
        arguments=', '.join(render_argument(argument) for argument in pg_aggregate.arguments),
        properties=',\n'.join(properties)
    )


def render_argument(pg_argument):
    if pg_argument.name is None:
        return str(pg_argument.data_type.ident())
    else:
        return '{} {}'.format(
            quote_ident(pg_argument.name),
            str(pg_argument.data_type.ident())
        )


sql_renderers = {
    PgTable: render_table_sql,
    PgFunction: render_function_sql,
    PgView: render_view_sql,
    PgCompositeType: render_composite_type_sql,
    PgEnumType: render_enum_type_sql,
    PgAggregate: render_aggregate_sql
}


class SqlRenderer:
    def __init__(self):
        self.if_not_exists = False

    def render(self, out_file, database):
        graph = database_to_graph(database)

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

        for pg_object in database.objects:
            yield '\n'
            yield sql_renderers[type(pg_object)](pg_object)

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


def quote_ident(ident):
    return '"' + ident + '"'


def quote_string(string):
    return "'" + string + "'"


def escape_string(string):
    return string.replace("'", "''")
