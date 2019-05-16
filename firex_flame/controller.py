import json
import logging
import os
from pathlib import Path

from firex_flame.event_aggregator import frontend_tasks_by_uuid

logger = logging.getLogger(__name__)


def _write_json(file, data):
    with open(file, 'w') as f:
        json.dump(data, fp=f, sort_keys=True, indent=2)


class FlameAppController:

    def __init__(self, sio_server, run_metadata):
        self.sio_server = sio_server
        self.run_metadata = run_metadata

    def send_event(self, new_data_by_task_uuid):
        # Avoid sending events if there aren't fields the downstream cares about.
        update_data_by_uuid = frontend_tasks_by_uuid(new_data_by_task_uuid)
        update_data_by_uuid = {uuid: task for uuid, task in update_data_by_uuid.items() if task}
        # Only emit frontend events when the data model has changed.
        if update_data_by_uuid:
            self.sio_server.emit('tasks-update', update_data_by_uuid)

    def dump_data_model(self, tasks_by_uuid):
        model_root_dir = os.path.join(self.run_metadata['logs_dir'], 'flame_model')
        full_tasks_dir = os.path.join(model_root_dir, 'full-tasks')
        os.makedirs(full_tasks_dir)

        _write_json(os.path.join(model_root_dir, 'slim-tasks.json'), frontend_tasks_by_uuid(tasks_by_uuid))
        _write_json(os.path.join(model_root_dir, 'run-metadata.json'), self.run_metadata)

        for uuid, task in tasks_by_uuid.items():
            _write_json(os.path.join(full_tasks_dir, '%s.json' % uuid), task)

        Path(model_root_dir, '.model-complete').touch()
