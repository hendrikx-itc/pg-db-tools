from itertools import chain
from pg_db_tools import iter_join


def render_sql(data):
    return chain(*(iter_join(
        '\n',
        (
            render_table_sql(obj_data)
            for obj_data in data['tables']
        )
    )))


def quote_ident(ident):
    return '"' + ident + '"'


def quote_string(string):
    return "'" + string + "'"


def render_table_sql(data):
    yield (
        'CREATE TABLE {ident}\n'
        '(\n'
        '{columns_part}\n'
        ');\n'
    ).format(
        ident='{}.{}'.format(quote_ident(data['schema']), quote_ident(data['name'])),
        columns_part=',\n'.join(
            '  {} {}'.format(quote_ident(c['name']), c['data_type']) for c in data['columns']
        )
    )

    if 'description' in data:
        yield (
            '\n'
            'COMMENT ON TABLE {} IS {};\n'
        ).format(quote_ident(data['name']), quote_string(data['description']))
