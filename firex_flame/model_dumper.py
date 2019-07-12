import json
import logging
import os
from pathlib import Path
import tarfile

from firex_flame.event_aggregator import slim_tasks_by_uuid, COMPLETE_STATES

logger = logging.getLogger(__name__)


def _write_json(file, data):
    with open(file, 'w') as f:
        json.dump(data, fp=f, sort_keys=True, indent=2)


def get_flame_model_dir(firex_logs_dir):
    return os.path.join(firex_logs_dir, 'flame_model')


def _get_base_model_dir(firex_logs_dir=None, root_model_dir=None):
    assert bool(firex_logs_dir) ^ bool(root_model_dir), \
        "Need exclusively either logs dir or root model dir."
    if firex_logs_dir:
        return get_flame_model_dir(firex_logs_dir)
    else:
        return root_model_dir


def get_all_tasks_dir(firex_logs_dir=None, root_model_dir=None):
    return os.path.join(_get_base_model_dir(firex_logs_dir, root_model_dir), 'full-tasks')


def get_tasks_slim_file(firex_logs_dir=None, root_model_dir=None):
    return os.path.join(_get_base_model_dir(firex_logs_dir, root_model_dir), 'slim-tasks.json')


def load_slim_tasks(log_dir):
    return json.loads(Path(get_tasks_slim_file(log_dir)).read_text())


def load_full_task(log_dir, task_uuid):
    return json.loads(Path(get_all_tasks_dir(firex_logs_dir=log_dir), task_uuid + '.json').read_text())


def get_full_tasks_by_names(log_dir, task_names):
    if isinstance(task_names, str):
        task_names = [task_names]
    tasks_by_uuid = get_full_tasks_by_slim_pred(log_dir, lambda st: st.get('name', None) in task_names)

    tasks_by_name = {n: [] for n in task_names}
    for t in tasks_by_uuid.values():
        tasks_by_name[t['name']].append(t)
    return tasks_by_name


def get_full_tasks_by_slim_pred(log_dir, slim_predicate):
    return {uuid: load_full_task(log_dir, uuid)
            for uuid, slim_task in load_slim_tasks(log_dir).items()
            if slim_predicate(slim_task)}


class FlameModelDumper:

    def __init__(self, firex_logs_dir=None, root_model_dir=None):
        assert bool(firex_logs_dir) ^ bool(root_model_dir), \
            "Dumper needs exclusively either logs dir or root model dir."
        if firex_logs_dir:
            self.root_model_dir = get_flame_model_dir(firex_logs_dir)
        else:
            self.root_model_dir = root_model_dir
        os.makedirs(self.root_model_dir, exist_ok=True)

    def dump_metadata(self, run_metadata, run_complete, flame_complete):
        metadata_model_file = os.path.join(self.root_model_dir, 'run-metadata.json')
        complete = {'run_complete': run_complete, 'flame_recv_complete': flame_complete}
        _write_json(metadata_model_file, {**complete, **run_metadata})
        return metadata_model_file

    def dump_complete_data_model(self, event_aggregator, run_metadata=None):
        logger.info("Starting to dump complete Flame model.")
        full_tasks_dir = get_all_tasks_dir(root_model_dir=self.root_model_dir)
        os.makedirs(full_tasks_dir)

        # Write JSON file with minimum amount of info to render graph.
        slim_tasks_file = get_tasks_slim_file(root_model_dir=self.root_model_dir)
        _write_json(slim_tasks_file, slim_tasks_by_uuid(event_aggregator.tasks_by_uuid))

        # Write one JSON file per task.
        for uuid, task in event_aggregator.tasks_by_uuid.items():
            _write_json(os.path.join(full_tasks_dir, '%s.json' % uuid), task)

        paths_to_compress = [slim_tasks_file, full_tasks_dir]
        if run_metadata:
            # Write metadata file.
            # Note that since a flame can terminate (e.g. via timeout) before a run, there is no guarantee
            # that the run_metadata model file will ever have run_complete: true.
            run_complete = event_aggregator.tasks_by_uuid.get(event_aggregator.root_uuid,
                                                              {'state': None})['state'] in COMPLETE_STATES
            metadata_model_file = self.dump_metadata(run_metadata, run_complete, flame_complete=True)
            paths_to_compress.append(metadata_model_file)

        # Write a tar.gz file containing all the files dumped above.
        with tarfile.open(os.path.join(self.root_model_dir, 'full-run-state.tar.gz'), "w:gz") as tar:
            for path in paths_to_compress:
                tar.add(path, arcname=os.path.basename(path))

        logger.info("Finished dumping complete Flame model.")
