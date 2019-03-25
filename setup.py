#!/usr/bin/env python3
import setuptools

with open("README.rst", "r") as fh:
    long_description = fh.read()


setuptools.setup(
    name='pg-db-tools',
    version='1.0.3',
    description='PostgreSQL database schema design and maintenance tools',
    author='Alfred Blokland',
    author_email='alfred.blokland@hendrikx-itc.nl',
    url='http://www.hendrikx-itc.nl',
    long_description=long_description,
    packages=[
        'pg_db_tools',
        'pg_db_tools.commands'
    ],
    install_requires=[
        'PyYAML', 'jsonschema', 'networkx', 'Jinja2', 'psycopg2-binary'
    ],
    package_dir={
        '': 'src'
    },
    package_data={
        'pg_db_tools': ['spec.schema', 'doc_template']
    },
    scripts=[
        'scripts/db-schema'
    ]
)
