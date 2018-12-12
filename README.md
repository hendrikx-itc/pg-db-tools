# PostgreSQL Database Tools

Design, Create, Maintain and Document a Postgresql database using a yaml based 
description file.

## Getting Started

To get things started, check the prerequisites, download and install the 
tool and then we are ready to use it.

### Prerequisites

We are using pip3 to install the needed libraries.

```
pip3 install PyYAML jsonschema networkx Jinja2 psycopg2-binary
```

### Installation:

Download and install the tool.

```
git clone https://github.com/hendrikx-itc/pg-db-tools
cd pg-db-tools
sudo python3 setup.py install
```

## Usage
--------

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
