

def render_dot(data):
    yield 'digraph {\n'

    for schema_name, schema_data in data.items():
        for table_data in schema_data['tables']:
            yield render_table_node(schema_name, table_data)
            yield render_table_edges(schema_name, table_data)

    yield '}\n'


def render_table_node(schema_name, table_data):
    return (
        '{} [\n'
        '  shape = none\n'
        '  label = {}\n'
        ']\n'
    ).format(table_data['name'], render_table_html_label(table_data))


def render_table_edges(schema_name, table_data):
    return ''.join(
        '{node_name}:{port} -> {dest_node_name}:{dest_port}\n'.format(
            node_name=table_data['name'],
            port=foreign_key['columns'][0],
            dest_node_name=foreign_key['references']['table']['name'],
            dest_port=foreign_key['references']['columns'][0]
        )
        for foreign_key in table_data.get('foreign_keys', [])
    )


def render_table_html_label(data):
    return (
        '<<table border="1" cellspacing="0" cellborder="0">\n'
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
