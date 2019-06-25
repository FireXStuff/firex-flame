import json
import logging
import os

from firex_flame.event_aggregator import slim_tasks_by_uuid

logger = logging.getLogger(__name__)


def _write_json(file, data):
    with open(file, 'w') as f:
        json.dump(data, fp=f, sort_keys=True, indent=2)


class FlameModelDumper:

    def __init__(self, firex_logs_dir=None, root_model_dir=None):
        assert bool(firex_logs_dir) ^ bool(root_model_dir), \
            "Dumper needs exclusively either logs dir or root model dir."
        if firex_logs_dir:
            self.root_model_dir = os.path.join(firex_logs_dir, 'flame_model')
        else:
            self.root_model_dir = root_model_dir
        os.makedirs(self.root_model_dir, exist_ok=True)

    def dump_metadata(self, run_metadata, run_complete):
        metadata_model_file = os.path.join(self.root_model_dir, 'run-metadata.json')
        _write_json(metadata_model_file, {'run_complete': run_complete, **run_metadata})

    def dump_complete_data_model(self, tasks_by_uuid, run_metadata=None):
        full_tasks_dir = os.path.join(self.root_model_dir, 'full-tasks')
        os.makedirs(full_tasks_dir)

        _write_json(os.path.join(self.root_model_dir, 'slim-tasks.json'), slim_tasks_by_uuid(tasks_by_uuid))
        if run_metadata:
            self.dump_metadata(run_metadata, run_complete=True)

        for uuid, task in tasks_by_uuid.items():
            _write_json(os.path.join(full_tasks_dir, '%s.json' % uuid), task)

        # TODO tar & compress (gz? fb compress?)


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
