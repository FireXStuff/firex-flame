import os
import unittest

from firex_flame.flame_helper import query_full_tasks, query_partial_tasks

test_data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")


class TaskQueryTests(unittest.TestCase):

    def test_match_all(self):
        # Don't include parent_id in the result, field4 doesn't exist anywhere.
        selectPaths = ['uuid', 'field1', 'field2', 'field4']
        queries = [
            {
                'matchCriteria': {'type': 'all'},
                'selectPaths': selectPaths,
            }
        ]
        tasks = {
            '1': {'field1': 1, 'field2': 2, 'parent_id': None, 'uuid': '1'},
            '2': {'field1': 3, 'field2': 4, 'parent_id': '1', 'uuid': '2'},
        }

        result = query_full_tasks(tasks, queries)

        self.assertEqual(len(result), 2)
        self.assertEqual(result['1'], {k: v for k, v in tasks['1'].items() if k in selectPaths})
        self.assertEqual(result['2'], {k: v for k, v in tasks['2'].items() if k in selectPaths})

    def test_equals_criteria(self):
        queries = [
            {
                'matchCriteria': {
                    'type': 'equals',
                    'value': {'field1': 1}
                },
                'selectPaths': ['uuid'],
            }
        ]
        tasks = {
            '1': {'field1': 1, 'parent_id': None, 'uuid': '1'},
            '2': {'field1': 2, 'parent_id': '1', 'uuid': '2'},
        }

        result = query_full_tasks(tasks, queries)

        self.assertEqual(len(result), 1)
        self.assertEqual(result['1'], {'uuid': '1'})

    def test_nested_paths(self):
        queries = [
            {
                'matchCriteria': {'type': 'all'},
                'selectPaths': ['uuid', 'field1.2.3'],
            }
        ]
        tasks = {
            '1': {'field1': {'2': {'3': 1}}, 'parent_id': None, 'uuid': '1'},
            # There is no field1.2.3 value for task 2
            '2': {'field1': {'2': 2}, 'parent_id': '1', 'uuid': '2'},
        }

        result = query_full_tasks(tasks, queries)

        self.assertEqual(len(result), 2)
        self.assertEqual(result['1'], {'field1': {'2': {'3': 1}}, 'uuid': '1'})
        self.assertEqual(result['2'], {'uuid': '2'})

    def test_select_descendants(self):
        queries = [
            {
                'matchCriteria': {'type': 'always-select-fields'},
                'selectPaths': ['uuid', 'parent_id'],
            },
            {
                'matchCriteria': {
                    'type': 'equals',
                    'value': {'field1': 1}
                },
                'selectDescendants': [
                    {'type': 'equals', 'value': {'field1': 3}},
                ]
            }
        ]
        tasks = {
            '1': {'field1': 1, 'parent_id': None, 'uuid': '1'},
            '2': {'field1': 2, 'parent_id': '1', 'uuid': '2'},
            '3': {'field1': 3, 'parent_id': '2', 'uuid': '3'},
        }

        result = query_full_tasks(tasks, queries)

        self.assertEqual(len(result), 1)
        self.assertEqual(result['1'], {'parent_id': None, 'uuid': '1', 'descendants': {
            '3': {'parent_id': '2', 'uuid': '3'}
        }})

    def test_partial_select(self):
        queries = [
            {
                'matchCriteria': {'type': 'always-select-fields'},
                'selectPaths': ['uuid'],
            },
            {
                'matchCriteria': {
                    'type': 'equals',
                    'value': {'field1': 2}
                },
                'selectDescendants': [
                    {'type': 'equals', 'value': {'field1': 3}},
                ]
            },
            {
                'matchCriteria': {
                    'type': 'equals',
                    'value': {'field1': 3}
                },
            }
        ]
        all_tasks = {
            '1': {'field1': 1, 'parent_id': None, 'uuid': '1'},
            '2': {'field1': 2, 'parent_id': '1', 'uuid': '2'},
            '3': {'field1': 3, 'parent_id': '2', 'uuid': '3'},
            # Don't expect '4' in the result because it isn't included in list of UUIDs, even though 'field1' = 3
            '4': {'field1': 3, 'parent_id': '1', 'uuid': '4'},
        }

        result = query_partial_tasks(['3'], queries, all_tasks)

        self.assertEqual(result, {
            # '2' is included because it selects 3 as a descendant.
            '2': {'uuid': '2', 'descendants': {'3': {'uuid': '3'}}},
            '3': {'uuid': '3'},
        })
