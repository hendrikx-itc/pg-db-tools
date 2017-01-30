from itertools import chain
from pg_db_tools import iter_join


class SqlRenderer:
    def __init__(self):
        self.if_not_exists = False

    def render(self, data):
        return iter_join(
            '\n',
            chain(*(
                self.render_schema_sql(schema_name, schema_data)
                for schema_name, schema_data in data.items()
            ))
        )

    def render_schema_sql(self, schema_name, data):
        options = []

        if self.if_not_exists:
            options.append('IF NOT EXISTS')

        create_schema_statement = 'CREATE SCHEMA {options}{ident};\n'.format(
            options=''.join('{} '.format(option) for option in options),
            ident=quote_ident(schema_name)
        )

        return chain(
            [create_schema_statement],
            chain(*(
                self.render_table_sql(schema_name, table_data)
                for table_data in data['tables']
            ))
        )

    def render_table_sql(self, schema_name, data):
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
            ident='{}.{}'.format(quote_ident(schema_name), quote_ident(data['name'])),
            columns_part=',\n'.join(
                chain(
                    ('  {} {}'.format(quote_ident(c['name']), c['data_type']) for c in data['columns']),
                    ['  PRIMARY KEY ({})'.format(', '.join(data['primary_key']))],
                    ['  UNIQUE ({})'.format(', '.join(unique_constraint)) for unique_constraint in data.get('unique', [])]
                )
            )
        )

        if 'description' in data:
            yield (
                '\n'
                'COMMENT ON TABLE {} IS {};\n'
            ).format(
                quote_ident(data['name']),
                quote_string(escape_string(data['description']))
            )


def quote_ident(ident):
    return '"' + ident + '"'


def quote_string(string):
    return "'" + string + "'"


def escape_string(string):
    return string.replace("'", "''")
