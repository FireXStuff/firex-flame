import unittest

from firex_flame.event_aggregator import FlameEventAggregator

basic_event = {'uuid': '1', 'name': 'prefix.SomeTask', 'type': 'task-started'}
basic_event_added_fields = {
            'state': basic_event['type'],
            'long_name': basic_event['name'],
            'task_num': 1,
            'name': 'SomeTask',
        }


class EventAggregatorTests(unittest.TestCase):

    def test_add_new_task(self):
        aggregator = FlameEventAggregator()

        aggregator.aggregate_events([basic_event])

        expected_task = {**basic_event, **basic_event_added_fields}
        self.assertEqual({expected_task['uuid']: expected_task}, aggregator.tasks_by_uuid)

    def test_ignore_missing_uuid(self):
        aggregator = FlameEventAggregator()

        event = dict(basic_event)
        event.pop('uuid')

        aggregator.aggregate_events([event])
        self.assertEqual({}, aggregator.tasks_by_uuid)

    def test_no_copy_unknown_field(self):
        aggregator = FlameEventAggregator()

        event = dict(basic_event)
        unknown_key = '__fake__field'
        event[unknown_key] = 'value'

        aggregator.aggregate_events([event])
        self.assertTrue(unknown_key not in aggregator.tasks_by_uuid[event['uuid']])

    def test_merge_flame_data(self):
        aggregator = FlameEventAggregator()

        event1 = dict(basic_event)
        event1['flame_data'] = {
            'k1': {'k2': ['v1']},
        }
        event2 = {
            'uuid': event1['uuid'],
            'flame_data': {
                'k1': {'k2': ['v2']},
            }
        }
        expected_final_flame_data = {'k1': {'k2': ['v1', 'v2']}}

        aggregator.aggregate_events([event1])
        changed_data = aggregator.aggregate_events([event2])

        self.assertEqual({event1['uuid']: {'flame_data': expected_final_flame_data}}, changed_data)
        self.assertEqual(expected_final_flame_data, aggregator.tasks_by_uuid[event1['uuid']]['flame_data'])

