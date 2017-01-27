

def render_dot(data):
    yield 'digraph {\n'

    for obj_data in data['tables']:
        yield render_table_node(obj_data)

    yield '}\n'


def render_table_node(table_data):
    return (
        '{} [\n'
        '  shape = none\n'
        '  label = {}\n'
        ']\n'
    ).format(table_data['name'], render_table_html_label(table_data))


def render_table_html_label(data):
    return (
        '<<table border="0" cellspacing="0" cellborder="1">\n'
        '  <tr><td bgcolor="grey" colspan="2">{name}</td></tr>\n'
        '{column_rows}\n'
        '</table>>\n'
    ).format(
        name=data['name'],
        column_rows='\n'.join(
            '  <tr><td port="{col_name}" align="left">{col_name}</td>'
            '<td align="left">{data_type}</td></tr>'.format(col_name=c['name'], data_type=c.get('data_type', ''))
            for c in data['columns']
        )
    )
