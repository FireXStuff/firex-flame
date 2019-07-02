import json
import logging
import os
import tarfile

from firex_flame.event_aggregator import slim_tasks_by_uuid

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


class FlameModelDumper:

    def __init__(self, firex_logs_dir=None, root_model_dir=None):
        assert bool(firex_logs_dir) ^ bool(root_model_dir), \
            "Dumper needs exclusively either logs dir or root model dir."
        if firex_logs_dir:
            self.root_model_dir = get_flame_model_dir(firex_logs_dir)
        else:
            self.root_model_dir = root_model_dir
        os.makedirs(self.root_model_dir, exist_ok=True)

    def dump_metadata(self, run_metadata, run_complete):
        metadata_model_file = os.path.join(self.root_model_dir, 'run-metadata.json')
        _write_json(metadata_model_file, {'run_complete': run_complete, **run_metadata})
        return metadata_model_file

    def dump_complete_data_model(self, tasks_by_uuid, run_metadata=None):
        full_tasks_dir = get_all_tasks_dir(root_model_dir=self.root_model_dir)
        os.makedirs(full_tasks_dir)

        # Write JSON file with minimum amount of info to render graph.
        slim_tasks_file = get_tasks_slim_file(root_model_dir=self.root_model_dir)
        _write_json(slim_tasks_file, slim_tasks_by_uuid(tasks_by_uuid))

        # Write one JSON file per task.
        for uuid, task in tasks_by_uuid.items():
            _write_json(os.path.join(full_tasks_dir, '%s.json' % uuid), task)

        paths_to_compress = [slim_tasks_file, full_tasks_dir]
        if run_metadata:
            # Write metadata file, indicating the run is complete.
            metadata_model_file = self.dump_metadata(run_metadata, run_complete=True)
            paths_to_compress.append(metadata_model_file)

        # Write a tar.gz file containing all the files dumped above.
        with tarfile.open(os.path.join(self.root_model_dir, 'full-run-state.tar.gz'), "w:gz") as tar:
            for path in paths_to_compress:
                tar.add(path, arcname=os.path.basename(path))


class FlameAppController:

    def __init__(self, sio_server, run_metadata):
        self.sio_server = sio_server
        self.run_metadata = run_metadata
        self.model_dumper = FlameModelDumper(firex_logs_dir=self.run_metadata['logs_dir'])

    def send_slim_event(self, new_data_by_task_uuid):
        # Avoid sending events if there aren't fields the downstream cares about.
        update_data_by_uuid = {uuid: task
                               for uuid, task in slim_tasks_by_uuid(new_data_by_task_uuid).items()
                               if task}
        if update_data_by_uuid:
            self.sio_server.emit('tasks-update', update_data_by_uuid)

    def dump_initial_metadata(self):
        self.model_dumper.dump_metadata(self.run_metadata, run_complete=False)

    def dump_complete_data_model(self, tasks_by_uuid):
        self.model_dumper.dump_complete_data_model(tasks_by_uuid, self.run_metadata)
