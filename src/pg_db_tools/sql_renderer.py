from itertools import chain
from pg_db_tools import iter_join


def render_sql(data):
    return iter_join(
        '\n',
        chain(*(
            render_schema_sql(schema_name, schema_data)
            for schema_name, schema_data in data.items()
        ))
    )


def quote_ident(ident):
    return '"' + ident + '"'


def quote_string(string):
    return "'" + string + "'"


def escape_string(string):
    return string.replace("'", "''")


def render_schema_sql(schema_name, data):
    return chain(
        ['CREATE SCHEMA {};\n'.format(quote_ident(schema_name))],
        chain(*(
            render_table_sql(schema_name, table_data)
            for table_data in data['tables']
        ))
    )


def render_table_sql(schema_name, data):
    yield (
        'CREATE TABLE {ident}\n'
        '(\n'
        '{columns_part}\n'
        ');\n'
    ).format(
        ident='{}.{}'.format(quote_ident(schema_name), quote_ident(data['name'])),
        columns_part=',\n'.join(
            chain(
                ('  {} {}'.format(quote_ident(c['name']), c['data_type']) for c in data['columns']),
                ['  PRIMARY KEY ({})'.format(', '.join(data['primary_key']))]
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
