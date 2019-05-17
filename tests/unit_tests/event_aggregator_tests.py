import os
import unittest

from firex_flame.event_aggregator import FlameEventAggregator


class EventAggregatorTests(unittest.TestCase):

    def test_add_new_task(self):
        aggregator = FlameEventAggregator()

        event = {'uuid': '1', 'name': 'prefix.SomeTask', 'type': 'task-started'}

        aggregator.aggregate_events([event])

        added_fields = {'state': event['type'], 'long_name': event['name'], 'task_num': 1, 'name': 'SomeTask'}
        expected_task = {**event, **added_fields}
        self.assertEqual({expected_task['uuid']: expected_task}, aggregator.tasks_by_uuid)
