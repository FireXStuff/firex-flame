import os
import unittest

from firex_flame.flame_helper import FlameTaskGraph, find, _jsonpath_get_paths, \
    convert_json_paths_in_query, _container_from_json_paths_to_values

from firexapp.events.model import ADDITIONAL_CHILDREN_KEY
import jsonpath_ng

test_data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")


class TaskQueryTests(unittest.TestCase):

    def test_match_all(self):
        # Don't include parent_id in the result, field4 doesn't exist anywhere.
        selectPaths = ['uuid', 'field1', 'field2', 'field4']
        queries = convert_json_paths_in_query([
            {
                'matchCriteria': {'type': 'all'},
                'selectPaths': selectPaths,
            }
        ])
        tasks = {
            '1': {'field1': 1, 'field2': 2, 'parent_id': None, 'uuid': '1'},
            '2': {'field1': 3, 'field2': 4, 'parent_id': '1', 'uuid': '2'},
        }

        graph = FlameTaskGraph(tasks)
        result = graph.query_full_tasks(queries)

        self.assertEqual(len(result), 2)
        self.assertEqual(result['1'], {k: v for k, v in tasks['1'].items() if k in selectPaths})
        self.assertEqual(result['2'], {k: v for k, v in tasks['2'].items() if k in selectPaths})

    def test_equals_criteria(self):
        queries = convert_json_paths_in_query([
            {
                'matchCriteria': {
                    'type': 'equals',
                    'value': {'field1': 1}
                },
                'selectPaths': ['uuid'],
            }
        ])
        tasks = {
            '1': {'field1': 1, 'parent_id': None, 'uuid': '1'},
            '2': {'field1': 2, 'parent_id': '1', 'uuid': '2'},
        }

        result = FlameTaskGraph(tasks).query_full_tasks(queries)

        self.assertEqual(len(result), 1)
        self.assertEqual(result['1'], {'uuid': '1'})

    def test_nested_paths(self):
        queries = convert_json_paths_in_query([
            {
                'matchCriteria': {'type': 'all'},
                'selectPaths': ['uuid', 'field1.a.b'],
            }
        ])
        tasks = {
            '1': {'field1': {'a': {'b': 1}}, 'parent_id': None, 'uuid': '1'},
            # There is no field1.2.3 value for task 2
            '2': {'field1': {'2': 2}, 'parent_id': '1', 'uuid': '2'},
        }

        result = FlameTaskGraph(tasks).query_full_tasks(queries)

        self.assertEqual(len(result), 2)
        self.assertEqual(result['1'], {'field1': {'a': {'b': 1}}, 'uuid': '1'})
        self.assertEqual(result['2'], {'uuid': '2'})

    def test_select_descendants(self):
        queries = convert_json_paths_in_query([
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
        ])
        tasks = {
            '1': {'field1': 1, 'parent_id': None, 'uuid': '1'},
            '2': {'field1': 2, 'parent_id': '1', 'uuid': '2'},
            '3': {'field1': 3, 'parent_id': '2', 'uuid': '3'},
        }

        result = FlameTaskGraph(tasks).query_full_tasks(queries)

        self.assertEqual(len(result), 1)
        self.assertEqual(result['1'], {'parent_id': None, 'uuid': '1', 'descendants': {
            '3': {'parent_id': '2', 'uuid': '3'}
        }})

    def test_partial_select(self):
        queries = convert_json_paths_in_query([
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
        ])
        all_tasks = {
            '1': {'field1': 1, 'parent_id': None, 'uuid': '1'},
            '2': {'field1': 2, 'parent_id': '1', 'uuid': '2'},
            '3': {'field1': 3, 'parent_id': '2', 'uuid': '3'},
            # Don't expect '4' in the result because it isn't included in list of UUIDs, even though 'field1' = 3
            '4': {'field1': 3, 'parent_id': '1', 'uuid': '4'},
        }

        result = FlameTaskGraph(all_tasks).query_partial_tasks(['3'], queries)

        self.assertEqual(result, {
            # '2' is included because it selects 3 as a descendant.
            '2': {'uuid': '2', 'descendants': {'3': {'uuid': '3'}}},
            '3': {'uuid': '3'},
        })

    def test_select_from_inconsistent_additional_children(self):
        # Additional children can create inconsistencies in the graph, like a task being it's own descendant and
        # it's own ancestor. Make sure these weird cases work.
        queries = convert_json_paths_in_query([
            {
                'matchCriteria': {'type': 'always-select-fields'},
                'selectPaths': ['uuid'],
            },
            {
                'matchCriteria': {
                    'type': 'equals',
                    'value': {'uuid': '3'}
                },
                'selectDescendants': [
                    {'type': 'equals',
                     'value': {'field': 3}},
                ]
            },
            {
                'matchCriteria': {
                    'type': 'equals',
                    'value': {'field1': 3}
                },
            }
        ])
        all_tasks = {
            '1': {'field': 3, 'parent_id': None, 'uuid': '1'},
            '2': {'field': 3, 'parent_id': '1', 'uuid': '2'},
            '3': {
                'field': 3, 'parent_id': '2', 'uuid': '3',
                # make self a child and the parent a child.
                ADDITIONAL_CHILDREN_KEY: ['2', '3', '5'],
            },
            '4': {'field': 3, 'parent_id': '1', 'uuid': '4'},
            '5': {'field': 3, 'parent_id': '4', 'uuid': '5'},
            '6': {'field': 3, 'parent_id': '5', 'uuid': '6'},
            # not in results b/c field != 3
            '7': {'field': 2, 'parent_id': '3', 'uuid': '7'},

            # test multiple levels of ADDITIONAL_CHILDREN_KEY traversal
            '8': {
                'field': 3, 'parent_id': '5', 'uuid': '8',
                ADDITIONAL_CHILDREN_KEY: ['9'],
            },
            '9': {'field': 3, 'parent_id': '8', 'uuid': '9'},
        }

        partial_query_result = FlameTaskGraph(all_tasks).query_partial_tasks(['3'], queries)
        expected = {
            '3': {
                'uuid': '3',
                'descendants': {
                    # Find 3's parent, since it's an additional_child.
                    '2': {'uuid': '2'},
                    # Find 3 itself, since it's an additional_child.
                    '3': {'uuid': '3'},
                    '5': {'uuid': '5'},
                    '6': {'uuid': '6'},
                    '8': {'uuid': '8'},
                    '9': {'uuid': '9'},
                }},
        }
        self.assertEqual(partial_query_result, expected)

        full_query_result = FlameTaskGraph(all_tasks).query_full_tasks(queries)
        self.assertEqual(full_query_result, expected)

    def test_data_from_json_paths(self):
        self.assertEqual(
            _container_from_json_paths_to_values(
                {'a': 1},
            ),
            {'a': 1},
        )
        self.assertEqual(
            _container_from_json_paths_to_values(
                {'a.[0]': 1},
            ),
            {'a': [1]},
        )
        self.assertEqual(
            _container_from_json_paths_to_values(
                {
                    'a.[2].b': 1,
                    'a.[100].b': 2,
                    'a.[0].c': 0,
                    'b': 3,
                },
            ),
            {'a': [{'c': 0}, {'b': 1}, {'b': 2}], 'b': 3},
        )


    def test_select_list_from_task(self):
        task = {'field1': {'list': [{'ignored': 1, 'field2': 1}, {'field2': 2, 'field3': 3}]}}
        result = _jsonpath_get_paths(
            [
                jsonpath_ng.parse('$.field1.list[*].field2'),
                jsonpath_ng.parse('$.field1.list[*].field3'),
            ],
            task)

        self.assertEqual(
            result,
            {'field1': {'list': [
                {'field2': 1},
                {'field2': 2, 'field3': 3}
            ]}},
        )

    def test_select_list(self):
        queries = convert_json_paths_in_query([
            {
                'matchCriteria': {
                    'type': 'always-select-fields'
                },
                'selectPaths': [
                    'uuid',
                    'args.list_name[*].field1',
                    'args.list_name[*].field2.inner',
                ],
            },
            {
                'matchCriteria': {
                    'type': 'all',
                },
            }
        ])
        all_tasks = {
            '1': {'parent_id': None, 'uuid': '1'},
            '2': {
                'field': 3,
                'parent_id': '1',
                'uuid': '2',
                'args': {
                    'list_name': [
                        {
                            'field1': 1,
                            'field2': {
                                'inner': 2,
                            },
                            'field3': 3,
                        },
                        {
                            'field1': 3,
                            'field2': 4, # not a dict, won't match
                            'field3': 4,
                        },
                    ]
                }
            },
        }

        query_result = FlameTaskGraph(all_tasks).query_full_tasks(queries)
        expected = {
            '1': {'uuid': '1'},
            '2': {
                'uuid': '2',
                'args': {
                    'list_name': [
                        {
                            'field1': 1,
                            'field2': {
                                'inner': 2,
                            },
                        },
                        {
                            'field1': 3,
                        },
                    ]
                }
            },
        }
        self.assertEqual(query_result, expected)