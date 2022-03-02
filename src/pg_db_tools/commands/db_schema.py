#!/usr/bin/env python3
import argparse

from pg_db_tools.commands import compile as compile_cmd
from pg_db_tools.commands import extract as extract_cmd
from pg_db_tools.commands import doc as doc_cmd
from pg_db_tools.commands import diff as diff_cmd


def main():
    parser = argparse.ArgumentParser()

    subparsers = parser.add_subparsers()

    compile_cmd.setup_command_parser(subparsers)
    extract_cmd.setup_command_parser(subparsers)
    doc_cmd.setup_command_parser(subparsers)
    diff_cmd.setup_command_parser(subparsers)

    args = parser.parse_args()

    if not hasattr(args, 'cmd'):
        parser.print_help()
    else:
        args.cmd(args)


if __name__ == '__main__':
    main()
