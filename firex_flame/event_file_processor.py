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
        event_aggregator.aggregate_events([event])
    # Kludge incomplete runstates that will never become terminal.
    event_aggregator.aggregate_events(event_aggregator.generate_incomplete_events())


def dumper_main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--rec', help='Recording file to construct model from.')
    parser.add_argument('--dest_dir', help='Directory to which model should be dumped.')

    args = parser.parse_args()

    aggregator = FlameEventAggregator()
    process_recording_file(aggregator, args.rec)
    dump_data_model(args.dest_dir, aggregator.tasks_by_uuid)
