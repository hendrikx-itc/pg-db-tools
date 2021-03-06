#!/usr/bin/env python3
import argparse

from pg_db_tools.commands import compile
from pg_db_tools.commands import extract
from pg_db_tools.commands import doc
from pg_db_tools.commands import diff


def main():
    parser = argparse.ArgumentParser()

    subparsers = parser.add_subparsers()

    compile.setup_command_parser(subparsers)
    extract.setup_command_parser(subparsers)
    doc.setup_command_parser(subparsers)
    diff.setup_command_parser(subparsers)

    args = parser.parse_args()

    if not hasattr(args, 'cmd'):
        parser.print_help()
    else:
        args.cmd(args)


if __name__ == '__main__':
    main()
