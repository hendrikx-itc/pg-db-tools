from itertools import chain
from typing import Generator, List

from pg_db_tools import iter_join
from pg_db_tools.graph import database_to_graph
from pg_db_tools.pg_types import (
    PgEnumType,
    PgTable,
    PgFunction,
    PgView,
    PgCompositeType,
    PgAggregate,
    PgSequence,
    PgSchema,
    PgRole,
    PgTrigger,
    PgCast,
    PgSetting,
    PgRow,
    PgQuery,
    PgOperator,
    PgArgument,
    PgColumn,
    PgProcedure,
)
from pg_db_tools.modification import DropColumn, AddColumn


def render_setting_sql(pg_setting) -> List[str]:
    return [
        "DO $$ BEGIN",
        "EXECUTE 'ALTER DATABASE ' || current_database() || ' "
        "SET {} TO {}';".format(pg_setting.name, pg_setting.value),
        "END; $$;\n",
        "SET {} TO {};".format(pg_setting.name, pg_setting.value),
    ]


def render_query_sql(pg_query) -> List[str]:
    if pg_query.select:
        query = "SELECT {}".format(pg_query.query)
    else:
        query = pg_query.query
    if pg_query.from_table:
        return ["{} FROM {};".format(query, pg_query.from_table)]
    else:
        return ["{};".format(query)]


def render_table_sql(table) -> Generator[str, None, None]:
    options = []
    post_options = []

    if table.partition_type:
        post_options.append(
            "PARTITION BY {} ({})".format(
                table.partition_type.upper(), ",".join(table.partition_columns)
            )
        )

    if table.inherits:
        post_options.append(
            "INHERITS ({}.{})".format(
                quote_ident(table.inherits.schema.name),
                quote_ident(table.inherits.name),
            )
        )

    yield (
        "CREATE {persistence}TABLE {options}{ident}\n"
        "(\n"
        "{columns_part}\n"
        "){post_options};\n"
    ).format(
        persistence=(
            "" if table.persistence == "permanent" else table.persistence.upper() + " "
        ),
        options="".join("{} ".format(option) for option in options),
        ident="{}.{}".format(quote_ident(table.schema.name), quote_ident(table.name)),
        columns_part=",\n".join(table_defining_components(table)),
        post_options=" ".join(post_options),
    )

    if table.description:
        yield ("COMMENT ON TABLE {} IS {};\n").format(
            "{}.{}".format(quote_ident(table.schema.name), quote_ident(table.name)),
            quote_string(escape_string(table.description)),
        )

    for column in table.columns:
        if column.description:
            yield ("COMMENT ON COLUMN {}.{}.{} IS {};\n").format(
                quote_ident(table.schema.name),
                quote_ident(table.name),
                quote_ident(column.name),
                quote_string(escape_string(column.description)),
            )

    if table.indexes:
        for index in table.indexes:
            yield (
                'CREATE{} INDEX "{}" ON {}.{} USING {};\n'.format(
                    " UNIQUE" if index.unique else "",
                    index.name,
                    quote_ident(table.schema.name),
                    quote_ident(table.name),
                    index.definition,
                )
            )

    if table.owner:
        yield (
            "ALTER TABLE {}.{} OWNER TO {};\n".format(
                quote_ident(table.schema.name),
                quote_ident(table.name),
                table.owner.name,
            )
        )

    for role, grants in table.privileges:
        yield (
            "GRANT {} ON TABLE {}.{} TO {};\n".format(
                grants, quote_ident(table.schema.name), quote_ident(table.name), role
            )
        )

    for query in table.queries:
        for line in render_query_sql(query):
            yield line


def render_drop_table_sql(table: PgTable) -> str:
    return "DROP TABLE {}.{};".format(
        quote_ident(table.schema.name), quote_ident(table.name)
    )


def table_defining_components(table: PgTable) -> Generator[str, None, None]:
    for column_data in table.columns:
        if table.inherits and table.inherits.has_comparable_column(column_data):
            # We already have this from inheritance, so don't need to define
            continue
        yield "  {}".format(render_column_definition(column_data))

    if table.primary_key:
        yield "  PRIMARY KEY ({})".format(", ".join(table.primary_key.columns))

    if table.unique:
        for unique_constraint in table.unique:
            yield "  UNIQUE ({})".format(", ".join(unique_constraint["columns"]))

    for check in table.checks:
        if check.name:
            yield "  CONSTRAINT {} CHECK {}".format(
                quote_ident(check.name), check.expression
            )
        else:
            yield "  CHECK {}".format(check.expression)

    if table.exclude:
        for exclude_constraint in table.exclude:
            yield "  {}".format(render_exclude_constraint(exclude_constraint))


def render_column_definition(column: PgColumn) -> str:
    parts = [quote_ident(column.name), str(column.data_type)]

    if column.nullable is False:
        parts.append("NOT NULL")

    if column.default:
        parts.append("DEFAULT {}".format(column.default))

    if column.generated_identity is not None:
        if column.generated_identity == "by_default":
            parts.append("GENERATED BY DEFAULT AS IDENTITY")
        elif column.generated_identity == "always":
            parts.append("GENERATED ALWAYS AS IDENTITY")

    return " ".join(parts)


def render_composite_type_column_definition(column: PgColumn) -> str:
    return "{} {}".format(quote_ident(column.name), column.data_type)


def render_exclude_constraint(exclude_data) -> str:
    parts = ["EXCLUDE "]

    if exclude_data.get("index_method"):
        parts.append("USING {index_method} ".format(**exclude_data))

    parts.append(
        "({})".format(
            ", ".join(
                "{exclude_element} WITH {operator}".format(**e)
                for e in exclude_data["exclusions"]
            )
        )
    )

    return "".join(parts)


def render_function_sql(
    pg_function: PgFunction, replace=False
) -> Generator[str, None, None]:
    returns_part = "    RETURNS "

    table_arguments = [
        argument for argument in pg_function.arguments if argument.mode == "t"
    ]

    if table_arguments:
        returns_part += "TABLE({})".format(
            ", ".join(render_argument(argument) for argument in table_arguments)
        )
    else:
        if pg_function.returns_set:
            returns_part += "SETOF "

        returns_part += pg_function.return_type.ident()

    if replace:
        create_part = "CREATE OR REPLACE FUNCTION"
    else:
        create_part = "CREATE FUNCTION"

    yield (
        '{} "{}"."{}"({})'.format(
            create_part,
            pg_function.schema.name,
            pg_function.name,
            ", ".join(
                render_argument(argument)
                for argument in pg_function.arguments
                if argument.mode in ("i", "o", "b", "v")
            ),
        )
    )
    yield returns_part
    yield "AS $function$" if "$$" in str(pg_function.src) else "AS $$"
    yield str(pg_function.src)
    yield "${}$ LANGUAGE {} {}{}{};".format(
        "function" if "$$" in str(pg_function.src) else "",
        pg_function.language,
        pg_function.volatility.upper(),
        " STRICT" if pg_function.strict else "",
        " SECURITY DEFINER" if pg_function.secdef else "",
    )

    if pg_function.description:
        yield '\nCOMMENT ON FUNCTION "{}"."{}"({}) IS {};'.format(
            pg_function.schema.name,
            pg_function.name,
            ", ".join(
                render_argument(argument)
                for argument in pg_function.arguments
                if argument.mode in ("i", "o", "b", "v")
            ),
            quote_string(escape_string(pg_function.description)),
        )

    for query in pg_function.queries:
        for line in render_query_sql(query):
            yield line


def render_procedure_sql(
    pg_procedure: PgProcedure, replace=False
) -> Generator[str, None, None]:
    if replace:
        create_part = "CREATE OR REPLACE PROCEDURE"
    else:
        create_part = "CREATE PROCEDURE"

    yield (
        '{} "{}"."{}"({})'.format(
            create_part,
            pg_procedure.schema.name,
            pg_procedure.name,
            ", ".join(render_argument(argument) for argument in pg_procedure.arguments),
        )
    )
    yield "AS $procedure$" if "$$" in str(pg_procedure.src) else "AS $$"
    yield str(pg_procedure.src)
    yield "${}$ LANGUAGE {};".format(
        "procedure" if "$$" in str(pg_procedure.src) else "", pg_procedure.language
    )

    if pg_procedure.description:
        yield '\nCOMMENT ON FUNCTION "{}"."{}"({}) IS {};'.format(
            pg_procedure.schema.name,
            pg_procedure.name,
            ", ".join(render_argument(argument) for argument in pg_procedure.arguments),
            quote_string(escape_string(pg_procedure.description)),
        )

    for query in pg_procedure.queries:
        for line in render_query_sql(query):
            yield line


def render_drop_function_sql(pg_function: PgFunction) -> str:
    args_part = ", ".join(
        str(argument.data_type.ident())
        for argument in pg_function.arguments
        if argument.mode in ("i", "o", "b", "v")
    )

    return 'DROP FUNCTION "{}"."{}"({});'.format(
        pg_function.schema.name, pg_function.name, args_part
    )


def render_drop_procedure_sql(pg_procedure: PgProcedure) -> str:
    args_part = ", ".join(
        str(argument.data_type.ident()) for argument in pg_procedure.arguments
    )

    return 'DROP PROCEDURE "{}"."{}"({});'.format(
        pg_procedure.schema.name, pg_procedure.name, args_part
    )


def render_trigger_sql(pg_trigger: PgTrigger) -> List[str]:
    when = "INSTEAD OF" if pg_trigger.when == "instead" else pg_trigger.when.upper()
    return [
        "CREATE TRIGGER {}".format(pg_trigger.name),
        "  {} {} ON {}".format(
            when, " OR ".join(pg_trigger.events).upper(), pg_trigger.table
        ),
        "  FOR EACH {}".format(pg_trigger.affecteach.upper()),
        "  EXECUTE PROCEDURE {}();".format(pg_trigger.function),
    ]


def render_drop_trigger_sql(pg_trigger: PgTrigger) -> str:
    return "DROP TRIGGER {} ON {};".format(pg_trigger.name, pg_trigger.table)


def render_sequence_sql(pg_sequence: PgSequence) -> List[str]:
    return [
        "CREATE SEQUENCE {}.{}".format(pg_sequence.schema.name, pg_sequence.name),
        "  START WITH {}".format(pg_sequence.start_value),
        "  INCREMENT BY {}".format(pg_sequence.increment),
        "  NO MINVALUE"
        if pg_sequence.minimum_value is None
        else "MINVALUE {}".format(pg_sequence.minimum_value),
        "  NO MAXVALUE"
        if pg_sequence.maximum_value is None
        else "MAXVALUE {}".format(pg_sequence.maximum_value),
        "  CACHE 1;",
    ]


def render_cast_sql(pg_cast: PgCast) -> List[str]:
    return [
        "CREATE CAST ({} AS {})\n  WITH FUNCTION {}({}){};".format(
            pg_cast.source,
            pg_cast.target,
            pg_cast.function,
            pg_cast.source,
            " AS IMPLICIT" if pg_cast.implicit else "",
        )
    ]


def render_operator_sql(pg_operator: PgOperator) -> List[str]:
    result = [
        "CREATE OPERATOR {} (".format(pg_operator.name),
        "    PROCEDURE = {},".format(pg_operator.code),
    ]
    if pg_operator.lefttype:
        result.append("    LEFTARG = {},".format(pg_operator.lefttype.ident()))
    if pg_operator.righttype:
        result.append("    RIGHTARG = {},".format(pg_operator.righttype.ident()))
    result[-1] = result[-1].rstrip(",")
    result.append(");")
    return result


def render_drop_operator_sql(pg_operator: PgOperator) -> str:
    return "DROP OPERATOR {} ({}, {});".format(
        pg_operator.name, pg_operator.lefttype.ident(), pg_operator.righttype.ident()
    )


def render_row_sql(pg_row) -> List[str]:
    return [
        "INSERT INTO {} ({}) VALUES ({});".format(  # nosec B608
            pg_row.table,
            ", ".join(pg_row.values.keys()),
            ", ".join(
                [
                    "'{}'".format(x) if x == str(x) else "null" if x is None else str(x)
                    for x in pg_row.values.values()
                ]
            ),
        )
    ]


def render_role_sql(pg_role: PgRole) -> List[str]:
    attributes = (["LOGIN"] if pg_role.login else []) + [
        "SUPERUSER" if pg_role.super else "NOSUPERUSER",
        "INHERIT" if pg_role.inherit else "NOINHERIT",
        "CREATEDB" if pg_role.createdb else "NOCREATEDB",
        "CREATEROLE;" if pg_role.createrole else "NOCREATEROLE;",
    ]
    return (
        [
            "DO\n$$\nBEGIN",
            "  IF NOT EXISTS(SELECT * FROM pg_roles "  # nosec B608
            "WHERE rolname = '{}') THEN".format(pg_role.name),  # nosec B608
            "    CREATE ROLE {}".format(pg_role.name),
            "      " + " ".join(attribute for attribute in attributes),
            "  END IF;\nEND\n$$;",
        ]
        + [
            "\nGRANT {} TO {};".format(membership.name, pg_role.name)
            for membership in pg_role.membership
        ]
        + (
            ["\nCOMMENT ON ROLE {} IS '{}';".format(pg_role.name, pg_role.description)]
            if pg_role.description is not None
            else []
        )
    )


def render_view_sql(pg_view: PgView) -> Generator[str, None, None]:
    yield 'CREATE VIEW "{}"."{}" AS'.format(pg_view.schema.name, pg_view.name)
    yield pg_view.view_query

    grantees = {privilege[0] for privilege in pg_view.privileges}

    for grantee in grantees:
        yield "\nGRANT {} ON TABLE {}.{} TO {};".format(
            ",".join(
                [
                    privilege[1]
                    for privilege in pg_view.privileges
                    if privilege[0] == grantee
                ]
            ),
            quote_ident(pg_view.schema.name),
            quote_ident(pg_view.name),
            grantee,
        )

    for query in pg_view.queries:
        for line in render_query_sql(query):
            yield line


def render_drop_view_sql(pg_view: PgView) -> str:
    return 'DROP VIEW "{}"."{}"'.format(pg_view.schema.name, pg_view.name)


def render_composite_type_sql(
    pg_composite_type: PgCompositeType,
) -> Generator[str, None, None]:
    yield ("CREATE TYPE {ident} AS (\n" "{columns_part}\n" ");\n").format(
        ident="{}.{}".format(
            quote_ident(pg_composite_type.schema.name),
            quote_ident(pg_composite_type.name),
        ),
        columns_part=",\n".join(
            "  {}".format(render_composite_type_column_definition(column_data))
            for column_data in pg_composite_type.columns
        ),
    )


def render_drop_composite_type_sql(pg_composite_type: PgCompositeType) -> str:
    return "DROP TYPE {ident};".format(
        ident="{}.{}".format(
            quote_ident(pg_composite_type.schema.name),
            quote_ident(pg_composite_type.name),
        )
    )


def render_enum_type_sql(pg_enum_type: PgEnumType) -> Generator[str, None, None]:
    yield ("CREATE TYPE {ident} AS ENUM (\n" "{labels_part}\n" ");\n").format(
        ident="{}.{}".format(
            quote_ident(pg_enum_type.schema.name), quote_ident(pg_enum_type.name)
        ),
        labels_part=",\n".join(
            "  {}".format(quote_string(label)) for label in pg_enum_type.labels
        ),
    )


def render_aggregate_sql(pg_aggregate: PgAggregate) -> Generator[str, None, None]:
    properties = [
        "    sfunc = {}".format(pg_aggregate.sfunc.ident()),
        "    stype = {}".format(pg_aggregate.stype.ident()),
    ]

    yield ("CREATE AGGREGATE {ident} ({arguments}) (\n" "{properties}\n" ");\n").format(
        ident=pg_aggregate.ident(),
        arguments=", ".join(
            render_argument(argument) for argument in pg_aggregate.arguments
        ),
        properties=",\n".join(properties),
    )

    for query in pg_aggregate.queries:
        for line in render_query_sql(query):
            yield line


def render_argument(pg_argument: PgArgument) -> str:
    if pg_argument.name is None:
        return str(pg_argument.data_type.ident())
    else:
        return "{} {}{}".format(
            quote_ident(pg_argument.name),
            pg_argument.data_type.ident(),
            ""
            if pg_argument.default is None
            else " DEFAULT {}".format(pg_argument.default),
        )


def render_schema_sql(pg_schema: PgSchema) -> Generator[str, None, None]:
    yield ("CREATE SCHEMA IF NOT EXISTS {ident};").format(
        ident=quote_ident(pg_schema.name)
    )
    if pg_schema.comment:
        yield (
            "COMMENT ON SCHEMA {} IS {};".format(
                quote_ident(pg_schema.name),
                quote_string(escape_string(pg_schema.comment)),
            )
        )
    if pg_schema.owner:
        yield (
            "ALTER SCHEMA {} OWNER TO {};".format(
                quote_ident(pg_schema.name), pg_schema.owner.name
            )
        )
    for priv in pg_schema.privileges:
        yield (
            "GRANT {} ON SCHEMA {} TO {};".format(
                priv[1], quote_ident(pg_schema.name), quote_ident(priv[0].name)
            )
        )
    for priv in pg_schema.default_privileges:
        yield (
            "ALTER DEFAULT PRIVILEGES IN SCHEMA {} GRANT {} ON {} TO {};\n".format(
                quote_ident(pg_schema.name), priv[2], priv[1], quote_ident(priv[0].name)
            )
        )
    for query in pg_schema.queries:
        for line in render_query_sql(query):
            yield line

sql_renderers = {
    PgSetting: render_setting_sql,
    PgSchema: render_schema_sql,
    PgTable: render_table_sql,
    PgFunction: render_function_sql,
    PgProcedure: render_procedure_sql,
    PgSequence: render_sequence_sql,
    PgView: render_view_sql,
    PgCompositeType: render_composite_type_sql,
    PgEnumType: render_enum_type_sql,
    PgAggregate: render_aggregate_sql,
    PgTrigger: render_trigger_sql,
    PgRole: render_role_sql,
    PgCast: render_cast_sql,
    PgOperator: render_operator_sql,
    PgRow: render_row_sql,
    PgQuery: render_query_sql,
}


def render_drop_column(drop_column: DropColumn) -> str:
    return "ALTER TABLE {}.{} DROP COLUMN {};".format(
        quote_ident(drop_column.table.schema.name),
        quote_ident(drop_column.table.name),
        quote_ident(drop_column.column.name),
    )


def render_add_column(add_column: AddColumn) -> str:
    return "ALTER TABLE {}.{} ADD COLUMN {};".format(
        quote_ident(add_column.table.schema.name),
        quote_ident(add_column.table.name),
        render_column_definition(add_column.column),
    )


modification_renderers = {DropColumn: render_drop_column, AddColumn: render_add_column}


def render_modification(modification):
    renderer = modification_renderers.get(type(modification))

    return renderer(modification)


class SqlRenderer:
    def __init__(self):
        self.if_not_exists = True

    def render(self, out_file, database):

        graph = database_to_graph(database)

        rendered_chunks = self.render_chunks(database)

        out_file.writelines(rendered_chunks)

    def render_chunks(self, database):
        return iter_join("\n", chain(*self.render_chunk_sets(database)))

    def render_chunk_sets(self, database):
        yield self.create_extension_statements(database)

        for pg_object in database.objects:
            yield "\n"
            yield sql_renderers[type(pg_object)](pg_object)

        yield "\n"

        for schema in sorted(database.schemas.values(), key=lambda s: s.name):
            for table in schema.tables:
                for index, foreign_key in enumerate(table.foreign_keys):
                    yield SqlRenderer.render_foreign_key(
                        index, schema, table, foreign_key
                    )

    @staticmethod
    def render_foreign_key(index, schema, table, foreign_key):
        key_name = foreign_key.name or "{}_{}_fk_{}".format(
            schema.name, table.name, index
        )
        return [
            (
                "ALTER TABLE {schema_name}.{table_name}\n"
                "  ADD CONSTRAINT {key_name}\n"
                "  FOREIGN KEY ({columns})\n"
                "  REFERENCES {ref_schema_name}.{ref_table_name} "
                "({ref_columns}){on_update}{on_delete};\n"
            ).format(
                schema_name=quote_ident(schema.name),
                table_name=quote_ident(table.name),
                key_name=quote_ident(key_name),
                columns=", ".join(foreign_key.columns),
                ref_schema_name=quote_ident(foreign_key.get_name(foreign_key.schema)),
                ref_table_name=quote_ident(foreign_key.get_name(foreign_key.ref_table)),
                ref_columns=", ".join(foreign_key.ref_columns),
                on_update=" ON UPDATE {}".format(foreign_key.on_update.upper())
                if foreign_key.on_update
                else "",
                on_delete=" ON DELETE {}".format(foreign_key.on_delete.upper())
                if foreign_key.on_delete
                else "",
            )
        ]

    def create_extension_statements(self, database):
        options = []
        
        if self.if_not_exists:
            options.append("IF NOT EXISTS")

        for extension_name in database.extensions:
            yield "CREATE EXTENSION {options}{extension_name};\n".format(
                options="".join("{} ".format(option) for option in options),
                extension_name=extension_name,
            )


def quote_ident(ident) -> str:
    return '"' + ident + '"'


def quote_string(string) -> str:
    return "'" + string + "'"


def escape_string(string) -> str:
    return string.replace("'", "''")
