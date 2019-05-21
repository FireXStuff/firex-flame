import argparse
import os
import json

from firex_flame.controller import dump_data_model
from firex_flame.event_aggregator import FlameEventAggregator


def process_recording_file(event_aggregator, recording_file):
    assert os.path.isfile(recording_file), "Recording file doesn't exist: %s" % recording_file

    with open(recording_file) as rec:
        event_lines = rec.readlines()
    for event_line in event_lines:
        if not event_line:
            continue
        event = json.loads(event_line)
        # TODO: might need to do query-time task-type fixing, since here trailing in-progress tasks for which
        # we don't received task-revoked will appear in progress.
        event_aggregator.aggregate_events([event])


def dumper_main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--rec', help='Recording file to construct model from.')
    parser.add_argument('--dest_dir', help='Directory to which model should be dumped.')

    args = parser.parse_args()

    aggregator = FlameEventAggregator()
    process_recording_file(aggregator, args.rec)
    dump_data_model(args.dest_dir, aggregator.tasks_by_uuid)
