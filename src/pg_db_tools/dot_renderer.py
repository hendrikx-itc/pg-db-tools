

class DotRenderer:
    @staticmethod
    def render(out_file, database):
        rendered_chunks = render_dot_chunks(database)

        out_file.writelines(rendered_chunks)


def render_dot_chunks(database):
    yield 'digraph {\n'

    for schema_name, schema in database.schemas.items():
        for table_data in schema.tables:
            yield render_table_node(schema_name, table_data)
            yield render_table_edges(schema_name, table_data)

    yield '}\n'


def table_node_name(schema_name, table_name):
    return '{}_{}'.format(schema_name, table_name)


def render_table_node(schema_name, table):
    return (
        '{} [\n'
        '  shape = none\n'
        '  label = {}\n'
        ']\n'
    ).format(table_node_name(schema_name, table.name), render_table_html_label(table))


def render_table_edges(schema_name, table):
    return ''.join(
        '{node_name}:{port} -> {dest_node_name}:{dest_port}\n'.format(
            node_name=table_node_name(schema_name, table.name),
            port=foreign_key['columns'][0],
            dest_node_name=table_node_name(
                foreign_key['references']['table']['schema'],
                foreign_key['references']['table']['name']
            ),
            dest_port=foreign_key['references']['columns'][0]
        )
        for foreign_key in table.foreign_keys
    )


def render_table_html_label(table):
    return (
        '<<table border="1" cellspacing="0" cellborder="0">\n'
        '  <tr><td bgcolor="grey" colspan="2">{name}</td></tr>\n'
        '{column_rows}\n'
        '</table>>\n'
    ).format(
        name=table.name,
        column_rows='\n'.join(
            '  <tr><td port="{col_name}" align="left">{col_name}</td>'
            '<td align="left">{data_type}</td></tr>'.format(col_name=c.name, data_type=c.data_type)
            for c in table.columns
        )
    )
