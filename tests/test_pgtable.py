from pg_db_tools.pg_types import PgTable, PgColumn


def test_diff():
    table_a = PgTable("schema_a", "table_1", [PgColumn("name", "text")])
    table_b = PgTable("schema_b", "table_2", [PgColumn("name", "text")])
