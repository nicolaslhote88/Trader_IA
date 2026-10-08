import ast
import unittest
from pathlib import Path

SOURCE = Path(__file__).resolve().parents[1] / 'nodes' / '02_duckdb_init.py'


def rotation_function():
    tree = ast.parse(SOURCE.read_text(encoding='utf-8'))
    functions = [node for node in tree.body if isinstance(node, ast.FunctionDef)
                 and node.name in {'_entry_symbol', 'apply_rotation_mode'}]
    namespace = {}
    exec(compile(ast.Module(body=functions, type_ignores=[]), str(SOURCE), 'exec'), namespace)
    return namespace['apply_rotation_mode']


class CoverageRotationTests(unittest.TestCase):
    def test_coverage_includes_core_and_held_without_quarantine_or_unclassified(self):
        symbols = ['HELD_OK', 'CORE_AUTO_OK', 'CORE_MANUAL_OK', 'WATCH_OK', 'QUARANTINED', 'UNCLASSIFIED']
        queue = [{'symbol': s} for s in symbols]
        segments = dict(zip(symbols[:4], [{'HELD'}, {'CORE_AUTO'}, {'CORE_MANUAL'}, {'WATCHLIST'}]))
        segments['QUARANTINED'] = {'HELD', 'CORE_MANUAL'}
        always, rotated, meta = rotation_function()(queue, {'QUARANTINED'}, segments, 'COVERAGE', 80)
        self.assertEqual([], always)
        self.assertEqual(symbols[:4], [r['symbol'] for r in rotated])
        self.assertEqual(4, meta['segment_rotation_total'])

    def test_regular_held_review_retains_quarantined_positions(self):
        queue = [{'symbol': 'HELD_Q'}, {'symbol': 'CORE_Q'}, {'symbol': 'CORE_OK'}]
        segments = {'HELD_Q': {'HELD'}, 'CORE_Q': {'CORE_MANUAL'}, 'CORE_OK': {'CORE_AUTO'}}
        always, rotated, _ = rotation_function()(queue, {'HELD_Q', 'CORE_Q'}, segments, 'HELD_CORE', 18)
        self.assertEqual(['HELD_Q'], [r['symbol'] for r in always])
        self.assertEqual(['CORE_OK'], [r['symbol'] for r in rotated])


if __name__ == '__main__':
    unittest.main()
