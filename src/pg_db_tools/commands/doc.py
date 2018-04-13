from pg_db_tools.commands import doc_init
from pg_db_tools.commands import doc_generate


def setup_command_parser(subparsers):
    cmd = subparsers.add_parser(
        'doc', help='documentation generation commands'
    )

    cmd_subparsers = cmd.add_subparsers()

    doc_init.setup_command_parser(cmd_subparsers)
    doc_generate.setup_command_parser(cmd_subparsers)
