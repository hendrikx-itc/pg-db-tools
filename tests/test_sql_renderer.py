"""
Test the SQL rendering function for schema data.
"""
import unittest
from io import StringIO

from pg_db_tools.pg_types import load
from pg_db_tools.sql_renderer import SqlRenderer


json_data = """
extensions:
  - btree_gist

types:
  - enum:
      name: order_state
      schema: shop
      values:
        - new
        - shipped
        - cancelled

objects:
  - table:
      name: Order
      schema: shop
      description: "Contains all orders"
      columns:
        - name: id
          description: "Primary key"
          data_type: integer
        - name: created
          data_type: timestamp with time zone
          nullable: false
          default: now()
      primary_key:
        name: order_pkey
        columns: ['id']

  - table:
      name: OrderLine
      schema: shop
      description: "Contains all order lines for all orders"
      columns:
        - name: id
          description: "Primary key"
          data_type: integer
        - name: order_id
          data_type: integer
          nullable: false
        - name: line_nr
          data_type: integer
        - name: product_id
          data_type: integer
        - name: amount
          data_type: integer
      check:
        - expression: line_nr > 0
      foreign_keys:
        - columns:
            - order_id
          references:
            table:
              schema: shop
              name: Order
            columns:
              - id
"""


class TestSqlRenderer(unittest.TestCase):

    def test_render(self):
        database = load(StringIO(json_data))

        out = StringIO()

        renderer = SqlRenderer()

        renderer.render(out, database)

        out.seek(0)

        rendered_sql = out.read()

        self.assertTrue('"created" timestamp with time zone NOT NULL DEFAULT now()' in rendered_sql)

        self.assertTrue(len(rendered_sql) > 0)
