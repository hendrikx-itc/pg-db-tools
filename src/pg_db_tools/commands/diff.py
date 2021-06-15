"""
Provides the 'diff' sub-command including argument parsing
"""
import os
import sys

from pg_db_tools.pg_types import load, PgSchema, PgFunction
from pg_db_tools.sql_renderer import render_table_sql, render_function_sql, \
    render_drop_table_sql, render_drop_function_sql, render_trigger_sql, \
    render_drop_trigger_sql, render_composite_type_sql, \
    render_drop_composite_type_sql, render_drop_operator_sql, \
    render_operator_sql, render_modification, render_view_sql


def setup_command_parser(subparsers):
    """
    Sets up a new sub parser for the init command and adds it to the provided
    subparsers
    """
    parser = subparsers.add_parser(
        'diff', help='command initializing documentation file structure'
    )

    parser.add_argument(
        'current', help='current schema definition'
    )

    parser.add_argument(
        'target', help='target schema definition'
    )

    parser.set_defaults(cmd=diff_command)


def diff_command(args):
    """
    Entry point for the diff sub-command after parsing the arguments
    """
    if not os.path.isfile(args.current):
        print(f"No such file: {args.current}")
        return 1

    if not os.path.isfile(args.target):
        print(f"No such file: {args.target}")
        return 1

    with open(args.current) as current_file:
        current_db = load(current_file)

    with open(args.target) as target_file:
        target_db = load(target_file)

    diff_db(current_db, target_db)


def diff_db(current_db, target_db):
    for removed_operator in find_removed_operators(current_db, target_db):
        sys.stdout.write('\n')
        sys.stdout.write(render_drop_operator_sql(removed_operator))
        sys.stdout.write('\n')

    for new_operator in find_new_operators(current_db, target_db):
        sys.stdout.write('\n\n')

        for c in render_operator_sql(new_operator):
            sys.stdout.write(c)
            sys.stdout.write('\n')

    for name, target_schema in target_db.schemas.items():
        current_schema = current_db.schemas.get(name)

        if current_schema:
            diff_schema(current_schema, target_schema)
        else:
            print("add schema {}".format(name))

    for removed_trigger in find_removed_triggers(current_db, target_db):
        sys.stdout.write('\n')
        sys.stdout.write(render_drop_trigger_sql(removed_trigger))
        sys.stdout.write('\n')

    for new_trigger in find_new_triggers(current_db, target_db):
        sys.stdout.write('\n\n')

        for c in render_trigger_sql(new_trigger):
            sys.stdout.write(c)
            sys.stdout.write('\n')


def diff_schema(current_schema, target_schema):
    for current_function in find_removed_functions(current_schema, target_schema):
        sys.stdout.write('\n\n')
        sys.stdout.write(render_drop_function_sql(current_function))

    for current_table in find_removed_tables(current_schema, target_schema):
        sys.stdout.write('\n\n')
        sys.stdout.write(render_drop_table_sql(current_table))

    for current_view in find_removed_views(current_schema, target_schema):
        sys.stdout.write('\n\n')
        sys.stdout.write(render_drop_view_sql(current_view))

    for current_type in find_removed_types(current_schema, target_schema):
        sys.stdout.write(render_drop_composite_type_sql(current_type))

    for target_table in find_new_tables(current_schema, target_schema):
        sys.stdout.write('\n\n')

        for c in render_table_sql(target_table):
            sys.stdout.write(c)
            sys.stdout.write('\n')

    for target_view in find_new_views(current_schema, target_schema):
        sys.stdout.write('\n\n')

        for c in render_view_sql(target_view):
            sys.stdout.write(c)
            sys.stdout.write('\n')

    for diff in find_modified_tables(current_schema, target_schema):
        sys.stdout.write('\n\n')

        for modification in diff.steps:
            sys.stdout.write(render_modification(modification))
            sys.stdout.write('\n')

    for target_type in find_new_types(current_schema, target_schema):
        sys.stdout.write('\n\n')

        for c in render_composite_type_sql(target_type):
            sys.stdout.write(c)
            sys.stdout.write('\n')

    for target_function in find_new_functions(current_schema, target_schema):
        sys.stdout.write('\n\n')

        for c in render_function_sql(target_function):
            sys.stdout.write(c)
            sys.stdout.write('\n')

    for target_function in find_modified_functions(current_schema, target_schema):
        sys.stdout.write('\n\n')

        for c in render_function_sql(target_function, replace=True):
            sys.stdout.write(c)
            sys.stdout.write('\n')


def function_matches(current_function: PgFunction, target_function: PgFunction):
    if current_function.name != target_function.name:
        return False

    if len(current_function.arguments) != len(target_function.arguments):
        return False

    for current_argument, target_argument in zip(current_function.arguments, target_function.arguments):
        if current_argument.data_type != target_argument.data_type:
            return False

    if current_function.return_type != target_function.return_type:
        return False

    return True


def find_new_views(current_schema, target_schema):
    for target_view in target_schema.views:
        try:
            current_view = next(
                v
                for v in current_schema.views
                if v.name == target_view.name
            )
        except StopIteration:
            yield target_view
        else:
            pass


def find_removed_views(current_schema, target_schema):
    for current_view in current_schema.views:
        try:
            target_view = next(
                t
                for t in target_schema.views
                if t.name == current_view.name
            )
        except StopIteration:
            # Table not found in target schema
            yield current_view
        else:
            pass


def find_new_tables(current_schema, target_schema):
    # Look for new tables
    for target_table in target_schema.tables:
        try:
            current_table = next(
                t
                for t in current_schema.tables
                if t.name == target_table.name
            )
        except StopIteration:
            # No table found
            yield target_table
        else:
            pass


def find_removed_tables(current_schema, target_schema):
    # Look for tables to remove
    for current_table in current_schema.tables:
        try:
            target_table = next(
                t
                for t in target_schema.tables
                if t.name == current_table.name
            )
        except StopIteration:
            # Table not found in target schema
            yield current_table
        else:
            pass


def find_modified_tables(current_schema: PgSchema, target_schema: PgSchema):
    for current_table in current_schema.tables:
        try:
            target_table = next(
                t
                for t in target_schema.tables
                if t.name == current_table.name
            )
        except StopIteration:
            pass
        else:
            diff = current_table.diff(target_table)

            if diff is not None:
                yield diff


def find_new_functions(current_schema, target_schema):
    # Look for new functions
    for target_function in target_schema.functions:
        try:
            current_function = next(
                f
                for f in current_schema.functions
                if function_matches(f, target_function)
            )
        except StopIteration:
            # Function not found in current schema
            yield target_function
        else:
            pass


def find_removed_functions(current_schema, target_schema):
    # Look for functions to remove
    for current_function in current_schema.functions:
        try:
            target_function = next(
                f
                for f in target_schema.functions
                if function_matches(f, current_function)
            )
        except StopIteration:
            # Function not found in target schema
            yield current_function
        else:
            pass


def find_modified_functions(current_schema, target_schema):
    # Look for functions to remove
    for current_function in current_schema.functions:
        try:
            target_function = next(
                f
                for f in target_schema.functions
                if function_matches(f, current_function)
            )
        except StopIteration:
            pass
        else:
            if target_function.src != current_function.src:
                yield target_function


def find_new_triggers(current_db, target_db):
    # Look for new triggers
    for key, target_trigger in target_db.triggers.items():
        if key not in current_db.triggers:
            yield target_trigger


def find_removed_triggers(current_db, target_db):
    # Look for triggers to remove
    for key, current_trigger in current_db.triggers.items():
        if key not in target_db.triggers:
            yield current_trigger


def find_new_types(current_schema, target_schema):
    for target_type in target_schema.composite_types:
        try:
            current_type = next(
                t
                for t in current_schema.composite_types
                if t.name == target_type.name
            )
        except StopIteration:
            # Type not found in current schema
            yield target_type
        else:
            pass


def find_removed_types(current_schema, target_schema):
    for current_type in current_schema.composite_types:
        try:
            target_type = next(
                t
                for t in target_schema.composite_types
                if t.name == current_type.name
            )
        except StopIteration:
            # Type not found in target schema
            yield current_type
        else:
            pass


def find_removed_operators(current_db, target_db):
    # Look for operators to remove
    for key, current_operator in current_db.operators.items():
        if key not in target_db.operators:
            yield current_operator


def find_new_operators(current_db, target_db):
    # Look for new operators
    for key, target_operator in target_db.operators.items():
        if key not in current_db.operators:
            yield target_operator
