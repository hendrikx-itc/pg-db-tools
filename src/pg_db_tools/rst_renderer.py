import os
from functools import reduce

from pg_db_tools import iter_join


def render_rst_directory(out_dir, database):
    for schema_name, schema in database.schemas.items():
        out_file_path = os.path.join(out_dir, '{}.rst'.format(schema_name))

        with open(out_file_path, 'w') as out_file:
            out_file.writelines(render_rst_schema(schema))

    index_file_path = os.path.join(out_dir, 'index.rst')

    with open(index_file_path, 'w') as index_file:
        index_file.write(
            'Schema Reference\n'
            '================\n'
            '.. toctree::\n'
        )

        index_file.writelines(
            '    {}\n'.format(schema.name)
            for schema in database.schemas.values()
        )


def render_rst_file(out_file, database):
    rendered_chunks = render_rst_chunks(database)

    out_file.writelines(rendered_chunks)


def render_rst_chunks(database):
    for schema_name, schema in database.schemas.items():
        yield from render_rst_schema(schema)


def render_rst_schema(schema):
    yield '{}\n\n'.format(header(1, 'Schema ``{}``'.format(schema.name)))

    if schema.types:
        yield '{}\n'.format(header(2, 'Types'))

        for pg_type in schema.types:
            yield render_type(pg_type)

    if schema.tables:
        yield '{}\n'.format(header(2, 'Tables'))

        for table in schema.tables:
            yield render_table(table)

    if schema.functions:
        yield '{}\n'.format(header(2, 'Functions'))

        for pg_function in schema.functions:
            yield render_function(pg_function)

    if schema.sequences:
        yield '{}\n'.format(header(2, 'Sequences'))

        for pg_sequence in schema.sequences:
            yield render_sequence(pg_sequence)


header_level_symbol = {
    1: '=',
    2: '-',
    3: '^',
    4: '"'
}


def header(level, text):
    return (
        '{}\n'
        '{}\n'
    ).format(
        text,
        len(text) * header_level_symbol[level]
    )


def render_type(pg_type):
    if type(pg_type) is PgEnum:
        return render_enum(pg_type)
    else:
        raise NotImplementedError(
            'No rendering implemented for type {}'.format(type(pg_type))
        )


def render_enum(pg_enum):
    return (
        '{}\n'
        '\n'
        '{}\n'
        '\n'
    ).format(
        header(2, 'Enum ``{}``'.format(pg_enum.name)),
        '\n'.join(
            render_table_grid(
                ['Value'],
                [(value, ) for value in pg_enum.values]
            )
        )
    )


UNICODE_CHECKMARK = '\u2714'
UNICODE_CROSS = '\u2718'


def nullable_marker(nullable):
    if nullable:
        return UNICODE_CHECKMARK
    else:
        return UNICODE_CROSS


def render_function(pg_function):
    def render_argument(argument):
        if argument.name is None:
            return str(argument.data_type)
        else:
            return '{} {}'.format(argument.name, str(argument.data_type))

    return (
        '{}\n'
        '{}\n\n'
    ).format(
        header(
            3,
            '{}({})'.format(
                pg_function.name,
                ', '.join(
                    render_argument(argument)
                    for argument in pg_function.arguments
                )
            )
        ),
        '' if pg_function.description is None else pg_function.description
    )


def render_sequence(pg_sequence):
    return '{}(integer)\n\n'.format(pg_sequence.name)


def render_table(table):
    lines = [
        header(3, table.name)
    ]

    if table.description is not None:
        lines.append(table.description)

    lines.append('')

    lines.extend(
        render_table_grid(
            ['Column', 'Type', 'Nullable', 'Description'],
            [
                (
                    column.name,
                    column.data_type,
                    nullable_marker(column.nullable),
                    column.description or ''
                )
                for column in table.columns
            ]
        )
    )

    lines.append('')

    return ''.join('{}\n'.format(line) for line in lines)


def render_table_grid(header, rows):
    header_widths = list(map(len, header))

    def max_widths(widths, row):
        return [
            max(width, len(str(cell_value)))
            for width, cell_value in zip(widths, row)
        ]

    max_widths = reduce(max_widths, rows, header_widths)

    sep_line = render_sep_line('-', max_widths)
    header_sep_line = render_sep_line('=', max_widths)

    yield sep_line

    yield '| {} |'.format(
        ' | '.join(
            column_name.ljust(width)
            for column_name, width in zip(header, max_widths)
        )
    )

    yield header_sep_line

    for line in iter_join(
            sep_line,
            (
                '| {} |'.format(
                    ' | '.join(
                        str(cell_value).ljust(width)
                        for cell_value, width in zip(row, max_widths)
                    )
                )
                for row in rows
            )
    ):
        yield line

    yield sep_line


def render_sep_line(sep_char, widths):
    return '+{}+'.format(
        '+'.join((width + 2) * sep_char for width in widths)
    )
