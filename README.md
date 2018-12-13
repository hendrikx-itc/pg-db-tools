# PostgreSQL Database Tools

Design, Create, Maintain and Document a Postgresql database using a yaml based 
description file.

## Getting Started

To get things started, check the prerequisites, download and install the 
tool and then we are ready to use it.

### Installation:

Download and install the tool.

```
git clone https://github.com/hendrikx-itc/pg-db-tools
sudo pip3 install pg-db-tools
```

## Usage

```
db-schema <command> [options]

commands:
    compile       compile output from schema definition
    extract       extract schema definition from source
    doc           documentation generation command
    --help,-h     display help information   
```

### compile

```
db-schema compile <output-type> <schema-filename>

output-types:
    dot   Generate Graphviz DOT
    sql   Generate SQL
    md    Generate Markdown documentation
    rst   Generate reStructuresText documentation
```

### extract

```
db-schema extract <source> 

sources:
    from-db   Extract from postgres database
```

```
db-schema extract from-db --format {yaml.json} [--owner OWNER] [schemas]
```

schemas: 
* Multiple schemas can be supplied
* If no schema is supplied, all schemas are returned

Example:
```
PGHOST=localhost \
PGPORT=5432 \
PGUSER=postgres \
PGDATABASE=postgres \
db-schema extract from-db --format yaml
```

### doc


## Examples

Create sql from the example webshop.yaml
```
db-schema compile sql example/webshop.yaml
```

Create rst documentation from the example webshop.yaml
```
db-schema compile rst example/webshop.yaml
```
result:
```
Schema ``shop``
===============


Tables
------

Order
^^^^^

Contains all orders

+---------+--------------------------+----------+-------------+
| Column  | Type                     | Nullable | Description |
+=========+==========================+==========+=============+
| id      | integer                  | ✔        | Primary key |
+---------+--------------------------+----------+-------------+
| created | timestamp with time zone | ✔        |             |
+---------+--------------------------+----------+-------------+

OrderLine
^^^^^^^^^

Contains all order lines for all orders

+------------+---------+----------+-------------+
| Column     | Type    | Nullable | Description |
+============+=========+==========+=============+
| id         | integer | ✔        | Primary key |
+------------+---------+----------+-------------+
| order_id   | integer | ✔        |             |
+------------+---------+----------+-------------+
| line_nr    | integer | ✔        |             |
+------------+---------+----------+-------------+
| product_id | integer | ✔        |             |
+------------+---------+----------+-------------+
| amount     | integer | ✔        |             |
+------------+---------+----------+-------------+

Schema ``public``
=================
```


## Description Format

One of the main components of the toolset is a database schema description
format. The description format is based on YAML, because it is easy to read and
write for humans.

See an example [here](https://github.com/hendrikx-itc/pg-db-tools/blob/master/example/webshop.yaml)

See the schema file [here](https://github.com/hendrikx-itc/pg-db-tools/blob/master/src/pg_db_tools/spec.schema)

## Note


This tool is specifically not meant as a cross database toolset, because 
that usually causes compatibility headaches and multiple partially supported 
database engines.
