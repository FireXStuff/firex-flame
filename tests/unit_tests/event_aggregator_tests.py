import unittest

from firex_flame.event_aggregator import FlameEventAggregator

basic_event = {'uuid': '1', 'name': 'prefix.SomeTask', 'type': 'task-started', 'timestamp': 0}
basic_event_added_fields = {
            'state': basic_event['type'],
            'long_name': basic_event['name'],
            'task_num': 1,
            'name': 'SomeTask',
            'states': [{'state': 'task-started', 'timestamp': 0}],
        }


class EventAggregatorTests(unittest.TestCase):

    def test_add_new_task(self):
        aggregator = FlameEventAggregator()

        aggregator.aggregate_events([basic_event])

        expected_task = {**basic_event, **basic_event_added_fields}
        expected_task.pop('timestamp')
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

    def test_aggregate_states(self):
        aggregator = FlameEventAggregator()

        event1 = dict(basic_event)
        events = [
            event1,
            {'uuid': event1['uuid'], 'type': 'task-blocked', 'timestamp': 1},
            {'uuid': event1['uuid'], 'type': 'task-started', 'timestamp': 2},
            {'uuid': event1['uuid'], 'type': 'task-succeeded', 'timestamp': 3},
        ]

        aggregator.aggregate_events(events)

        aggregated_states = aggregator.tasks_by_uuid[event1['uuid']]['states']
        expected_states = [{'state': e['type'], 'timestamp': e['timestamp']} for e in events]
        print(aggregated_states)
        self.assertEqual(expected_states, aggregated_states)

    def test_capture_root(self):
        aggregator = FlameEventAggregator()

        event1 = {'parent_id': None, **basic_event}
        event2 = {**event1, 'parent_id': event1['uuid']}

        aggregator.aggregate_events([event1, event2])
        self.assertEqual(event1['uuid'], aggregator.root_uuid)

