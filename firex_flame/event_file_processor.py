import os
import json


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
