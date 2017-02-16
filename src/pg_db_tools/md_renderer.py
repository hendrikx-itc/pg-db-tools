def render_md(data):
    for schema_name, schema in data.items():
        yield '{}\n\n'.format(header(1, schema_name))

        for table_data in schema['tables']:
            yield render_table(table_data)


def header(level, text):
    mark = level * '#'

    return '{} {} {}'.format(mark, text, mark)


def render_table(table_data):
    return (
        '{}\n'
        '\n'
        '{}\n'
        '\n'
    ).format(
        header(2, table_data['name']),
        '\n'.join(
            '| {name} | {data_type} |'.format(**column)
            for column in table_data['columns']
        )
    )
