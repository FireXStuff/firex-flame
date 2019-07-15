import logging

from firex_flame.event_aggregator import slim_tasks_by_uuid
from firex_flame.model_dumper import FlameModelDumper

logger = logging.getLogger(__name__)


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
        self.model_dumper.dump_metadata(self.run_metadata, run_complete=False, flame_complete=False)

    def dump_complete_data_model(self, event_aggregator):
        self.model_dumper.dump_aggregator_complete_data_model(event_aggregator, self.run_metadata)
