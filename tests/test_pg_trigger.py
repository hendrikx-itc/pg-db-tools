import unittest

from pg_db_tools.pg_types import PgTrigger


class TestRstRenderer(unittest.TestCase):
    def test_analyze_type(self):
        tgtype = 0b00100010

        timing, events, affecteach = PgTrigger.analyze_type(tgtype)

        self.assertEqual(timing, 'before')

        self.assertEqual(events, ['truncate'])

        self.assertEqual(affecteach, 'statement')
