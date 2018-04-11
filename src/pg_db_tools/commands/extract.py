from pg_db_tools.commands import extract_from_db


def setup_command_parser(subparsers):
    cmd = subparsers.add_parser(
        'extract', help='extract schema definition from a source'
    )

    cmd_subparsers = cmd.add_subparsers()

    extract_from_db.setup_command_parser(cmd_subparsers)
