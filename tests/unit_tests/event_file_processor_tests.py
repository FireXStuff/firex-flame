import os
import unittest

from firex_flame.event_aggregator import FlameEventAggregator
from firex_flame.event_file_processor import process_recording_file, get_model_or_rec_full_task

test_data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")


class EventFileProcessorTests(unittest.TestCase):

    def test_process_rec_file(self):
        event_aggregator = FlameEventAggregator()

        recording_file = os.path.join(test_data_dir, 'nop', 'debug', 'flame', 'flame.rec')

        run_metadata = {}
        process_recording_file(event_aggregator, recording_file, run_metadata)

        self.assertEqual(2, len(event_aggregator.tasks_by_uuid), "Expected two tasks, root and nop.")
        # TODO: assert on metadata once args are emitted in celery events by firexapp.

    def test_rec_full_task(self):
        t = get_model_or_rec_full_task(os.path.join(test_data_dir, 'nop'), 'e33054ad-4a02-45ae-b9d3-f92eda6cdedd')
        self.assertEqual('nop', t['name'])
