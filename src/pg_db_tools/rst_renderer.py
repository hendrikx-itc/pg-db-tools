from functools import reduce

from pg_db_tools import iter_join


class RstRenderer:
    @staticmethod
    def render(out_file, database):
        rendered_chunks = render_rst_chunks(database)

        out_file.writelines(rendered_chunks)


def render_rst_chunks(database):
    for schema_name, schema in database.schemas.items():
        yield '{}\n\n'.format(header(1, schema_name))

        for table in schema.tables:
            yield render_table(table)


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


def render_table(table):
    return (
        '{}\n'
        '\n'
        '{}\n'
        '\n'
    ).format(
        header(2, table.name),
        '\n'.join(
            render_table_grid(
                ['name', 'data type'],
                [(column.name, column.data_type) for column in table.columns]
            )
        )
    )


def render_table_grid(header, rows):
    header_widths = list(map(len, header))

    def max_widths(widths, row):
        return [
            max(width, len(cell_value))
            for width, cell_value in zip(widths, row)
        ]

    max_widths = reduce(max_widths, rows, header_widths)

    sep_line = render_sep_line('-', max_widths)
    header_sep_line = render_sep_line('=', max_widths)

    yield sep_line

    yield '| {} |'.format(' | '.join(column_name.ljust(width) for column_name, width in zip(header, max_widths)))

    yield header_sep_line

    for line in iter_join(
            sep_line,
            (
                '| {} |'.format(
                    ' | '.join(cell_value.ljust(width) for cell_value, width in zip(row, max_widths))
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
