"""
Test the load function for schema data from a YAML file.
"""
import unittest
from io import StringIO

from pg_db_tools.pg_types import load


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
      primary_key: ['id']

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
        - name: line_nr
          data_type: integer
        - name: product_id
          data_type: integer
        - name: amount
          data_type: integer
      foreign_keys:
        - columns:
            - order_id
          references:
            table:
              name: Order
              schema: shop
            columns:
              - id
"""

class TestLoad(unittest.TestCase):

    def test_load(self):
        database = load(StringIO(json_data))

        self.assertEqual(len(database.schemas), 1)

        self.assertTrue('shop' in database.schemas)
