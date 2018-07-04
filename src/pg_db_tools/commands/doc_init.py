"""
Provides the 'init' sub-command including argument parsing
"""
import os
import errno
import io

from pkg_resources import resource_listdir, resource_stream, resource_isdir

from jinja2 import Template


def setup_command_parser(subparsers):
    """
    Sets up a new sub parser for the init command and adds it to the provided
    subparsers
    """
    parser_extract = subparsers.add_parser(
        'init', help='command initializing documentation file structure'
    )

    parser_extract.add_argument(
        'directory', default='doc', help='target directory for documentation'
    )

    parser_extract.set_defaults(cmd=init_command)


def init_command(args):
    """
    Entry point for the init sub-command after parsing the arguments
    """
    copy_resource_tree(args.directory, 'pg_db_tools', 'doc_template')


def copy_resource_tree(target_dir, package_name, resource_root,
                       resource_dir=''):
    try:
        os.makedirs(os.path.join(target_dir, resource_dir))
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise

    for resource in resource_listdir(package_name,
                                     os.path.join(resource_root,
                                                  resource_dir)):
        resource_path = os.path.join(resource_root, resource_dir, resource)

        if resource_isdir(package_name, resource_path):
            copy_resource_tree(target_dir, package_name, resource_root,
                               os.path.join(resource_dir, resource))
        else:
            stream = resource_stream(package_name, resource_path)

            template = Template(io.TextIOWrapper(stream, 'utf-8').read())

            target_path = os.path.join(target_dir, resource_dir, resource)

            data = {
                'project_name': 'MyProject',
                'author': 'The Author',
                'version': '1.0',
                'release': '1.0.0a1'
            }

            with open(target_path, 'wb') as out_file:
                out_file.write(template.render(**data).encode('utf-8'))
