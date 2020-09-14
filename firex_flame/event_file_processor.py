import argparse
import os
import gzip
import json

from firex_flame.model_dumper import FlameModelDumper, find_flame_model_dir, get_model_full_tasks_by_names, \
    index_tasks_by_names, get_full_tasks_by_slim_pred
from firex_flame.event_aggregator import FlameEventAggregator, TASK_ARGS
from firex_flame.flame_helper import find_rec_file, find


def process_recording_file(event_aggregator: FlameEventAggregator, recording_file: str, run_metadata: dict):
    assert os.path.isfile(recording_file), "Recording file doesn't exist: %s" % recording_file

    real_rec = os.path.realpath(recording_file)
    if real_rec.endswith('.gz'):
        with gzip.open(real_rec, 'rt', encoding='utf-8') as rec:
            event_lines = rec.readlines()
    else:
        with open(recording_file) as rec:
            event_lines = rec.readlines()

    for event_line in event_lines:
        if not event_line:
            continue
        event = json.loads(event_line)
        event_aggregator.aggregate_events([event])

    if event_aggregator.is_root_complete():
        # Kludge incomplete runstates that will never become terminal.
        event_aggregator.aggregate_events(event_aggregator.generate_incomplete_events())

    if run_metadata.get('uid', None) is None and event_aggregator.root_uuid is not None:
        root_task = event_aggregator.tasks_by_uuid[event_aggregator.root_uuid]
        run_metadata['uid'] = find([TASK_ARGS, 'chain_args', 'uid'], root_task)
        run_metadata['chain'] = find([TASK_ARGS, 'chain'], root_task)


def dumper_main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--rec', help='Recording file to construct model from.')
    parser.add_argument('--dest_dir', help='Directory to which model should be dumped.')

    args = parser.parse_args()

    aggregator = FlameEventAggregator()
    process_recording_file(aggregator, args.rec, {})
    FlameModelDumper(root_model_dir=args.dest_dir).dump_aggregator_complete_data_model(aggregator)


def get_tasks_from_rec_file(log_dir=None, rec_filepath=None, mark_incomplete=False):
    assert bool(log_dir) ^ bool(rec_filepath), "Need exclusively either log directory of rec_file path."
    if not rec_filepath:
        rec_file = find_rec_file(log_dir)
    else:
        rec_file = rec_filepath
    assert os.path.exists(rec_file), "Recording file not found: %s" % rec_file
    aggregator = FlameEventAggregator()
    process_recording_file(aggregator, rec_file, {})

    if mark_incomplete:
        aggregator.aggregate_events(aggregator.generate_incomplete_events())

    return aggregator.tasks_by_uuid, aggregator.root_uuid


def get_model_or_rec_full_tasks_by_names(logs_dir, task_names):
    if os.path.isdir(find_flame_model_dir(logs_dir)):
        return get_model_full_tasks_by_names(logs_dir, task_names)

    rec_file = find_rec_file(logs_dir)
    if os.path.isfile(rec_file):
        tasks_by_uuid, _ = get_tasks_from_rec_file(rec_filepath=rec_file)
        return index_tasks_by_names(tasks_by_uuid.values(), task_names)

    raise Exception("Found neither model directory or rec_file, no source of task data in: %s" % logs_dir)


def get_model_or_rec_full_tasks_by_uuids(logs_dir, uuids):
    if os.path.isdir(find_flame_model_dir(logs_dir)):
        return get_full_tasks_by_slim_pred(logs_dir, lambda st: st['uuid'] in uuids)

    rec_file = find_rec_file(logs_dir)
    if os.path.isfile(rec_file):
        tasks_by_uuid, _ = get_tasks_from_rec_file(rec_filepath=rec_file)
        return {u: t for u, t in tasks_by_uuid.items() if u in uuids}

    raise Exception("Found neither model directory or rec_file, no source of task data in: %s" % logs_dir)


def get_model_or_rec_full_task(logs_dir, uuid):
    task_by_uuid = get_model_or_rec_full_tasks_by_uuids(logs_dir, [uuid])
    return task_by_uuid[uuid]
