FOREIGN_KEY_EDGE_CONNECT_PORT = 1
FOREIGN_KEY_EDGE_CONNECT_NODE = 2


class DotRenderer:
    def __init__(self):
        self.label_foreign_key_edges = True
        self.foreign_key_edge_mode = FOREIGN_KEY_EDGE_CONNECT_NODE
        self.href_prefix = '#'

    def render(self, out_file, database):
        rendered_chunks = self.render_dot_chunks(database)

        out_file.writelines(rendered_chunks)

    def render_dot_chunks(self, database):
        yield 'digraph schema {\n'
        yield '  outputorder=edgesfirst\n'

        for schema in sorted(database.schemas.values(), key=lambda s: s.name):
            yield '  subgraph cluster_{} {{\n'.format(schema.name)
            yield '    label = "{}"\n'.format(schema.name)

            for table in schema.tables:
                yield self.render_table_node(table)

            yield '  }\n'

        for schema in database.schemas.values():
            for table in schema.tables:
                yield self.render_table_edges(table)

        yield '}\n'

    def render_table_node(self, table):
        return (
            '{indent}{node_name} [\n'
            '{indent}  shape = none\n'
            '{indent}  href = "{href}"\n'
            '{indent}  target = "_top"\n'
            '{indent}  label = {label}\n'
            '{indent}]\n'
        ).format(
            indent='    ',
            node_name=table_node_name(table.schema.name, table.name),
            href="{}{}".format(self.href_prefix, table.name.replace('_', '-')),
            label=self.render_table_html_label(table)
        )

    def render_table_edges(self, table):
        return ''.join(
            self.render_foreign_key(table, foreign_key)
            for foreign_key in table.foreign_keys
        )

    def render_foreign_key(self, table, foreign_key):
        attributes = {}

        if self.label_foreign_key_edges:
            attributes['label'] = '{port} = {dest_port}'.format(
                port=foreign_key.columns[0],
                dest_port=foreign_key.ref_columns[0]
            )

        if self.foreign_key_edge_mode == FOREIGN_KEY_EDGE_CONNECT_PORT:
            source = '{node_name}:{port}'.format(
                node_name=table_node_name(table.schema.name, table.name),
                port=foreign_key.columns[0]
            )
            target = '{dest_node_name}:{dest_port}'.format(
                dest_node_name=table_node_name(
                    foreign_key.ref_table.schema.name,
                    foreign_key.ref_table.name
                ),
                dest_port=foreign_key.ref_columns[0]
            )
        else:
            source = '{node_name}'.format(
                node_name=table_node_name(table.schema.name, table.name)
            )
            target = '{dest_node_name}'.format(
                dest_node_name=table_node_name(
                    foreign_key.ref_table.schema.name,
                    foreign_key.ref_table.name
                )
            )

        return '{indent}{source} -> {target} [ {attributes} ];\n'.format(
            indent='  ',
            source=source,
            target=target,
            attributes=' '.join(
                '{}="{}"'.format(key, value)
                for key, value in attributes.items()
            )
        )

    def render_table_html_label(self, table):
        return (
            '<<table border="1" bgcolor="white" cellspacing="0" '
            'cellborder="0">\n'
            '  <tr><td colspan="3" border="1" sides="B">{name}</td></tr>\n'
            '{column_rows}\n'
            '</table>>\n'
        ).format(
            name=table.name,
            column_rows='\n'.join(
                '  <tr><td>{attrs}</td>'
                '<td port="{col_name}" align="left">{col_name}</td>'
                '<td align="left">{data_type}</td></tr>'.format(
                    attrs='PK' if (
                        table.primary_key and
                        c.name in table.primary_key.columns
                    ) else '',
                    col_name=c.name,
                    data_type=c.data_type
                )
                for c in table.columns
            )
        )


def table_node_name(schema_name, table_name):
    return '{}_{}'.format(schema_name, table_name)
