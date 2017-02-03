from itertools import chain
from pg_db_tools import iter_join


class SqlRenderer:
    def __init__(self):
        self.if_not_exists = False

    def render(self, data):
        return iter_join(
            '\n',
            chain(
                self.create_schema_statements(data),
                *[
                    self.render_object_sql(object_data)
                    for object_data in data['objects']
                ]
            )
        )

    def create_schema_statements(self, data):
        options = []

        if self.if_not_exists:
            options.append('IF NOT EXISTS')

        for schema_name in self.collect_schema_names(data):
            yield 'CREATE SCHEMA {options}{ident};\n'.format(
                options=''.join('{} '.format(option) for option in options),
                ident=quote_ident(schema_name)
            )

    def collect_schema_names(self, data):
        return list(set(db_object['table']['schema'] for db_object in data['objects']))

    def render_object_sql(self, data):
        object_type, object_data = next(iter(data.items()))

        if object_type == 'table':
            return self.render_table_sql(object_data)
        else:
            raise Exception('Unsupported object type: {}'.format(object_type))

    def render_table_sql(self, data):
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
            ident='{}.{}'.format(quote_ident(data['schema']), quote_ident(data['name'])),
            columns_part=',\n'.join(self.table_defining_components(data))
        )

        if 'description' in data:
            yield (
                '\n'
                'COMMENT ON TABLE {} IS {};\n'
            ).format(
                quote_ident(data['name']),
                quote_string(escape_string(data['description']))
            )

    def table_defining_components(self, table_data):
        for column_data in table_data['columns']:
            yield '  {}'.format(self.render_column_definition(column_data))

        if 'primary_key' in table_data:
            yield '  PRIMARY KEY ({})'.format(', '.join(table_data['primary_key']))

        for unique_constraint in table_data.get('unique', []):
            yield '  UNIQUE ({})'.format(', '.join(unique_constraint['columns']))

        for exclude_constraint in table_data.get('exclude', []):
            yield '  {}'.format(self.render_exclude_constraint(exclude_constraint))

    def render_column_definition(self, column_data):
        column_constraints = []

        if not column_data.get('nullable', False):
            column_constraints.append('NOT NULL')

        return '{} {}'.format(
            quote_ident(column_data['name']),
            column_data['data_type']
        )

    def render_exclude_constraint(self, exclude_data):
        parts = ['EXCLUDE ']

        if exclude_data.get('index_method'):
            parts.append('USING {index_method} '.format(**exclude_data))

        parts.append(
            '({})'.format(', '.join('{exclude_element} WITH {operator}'.format(**e) for e in exclude_data['exclusions']))
        )

        return ''.join(parts)


def quote_ident(ident):
    return '"' + ident + '"'


def quote_string(string):
    return "'" + string + "'"


def escape_string(string):
    return string.replace("'", "''")
