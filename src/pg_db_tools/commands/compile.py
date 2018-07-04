from pg_db_tools.commands import compile_dot, compile_md,\
    compile_rst, compile_sql


def setup_command_parser(subparsers):
    cmd = subparsers.add_parser(
        'compile', help='compile output from schema definition'
    )

    cmd_subparsers = cmd.add_subparsers()

    compile_dot.setup_command_parser(cmd_subparsers)
    compile_sql.setup_command_parser(cmd_subparsers)
    compile_md.setup_command_parser(cmd_subparsers)
    compile_rst.setup_command_parser(cmd_subparsers)
