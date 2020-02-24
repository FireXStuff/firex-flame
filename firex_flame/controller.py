import logging

from firex_flame.event_aggregator import slim_tasks_by_uuid
from firex_flame.model_dumper import FlameModelDumper

logger = logging.getLogger(__name__)


class FlameAppController:

    def __init__(self, run_metadata, extra_task_representations):

        self.run_metadata = run_metadata
        self.model_dumper = FlameModelDumper(firex_logs_dir=self.run_metadata['logs_dir'])
        self.extra_task_representations = extra_task_representations

        self.sio_server = None  # Set after creation.

    def send_slim_event(self, new_data_by_task_uuid):
        # sio_server can be lazy initialized. Since the event receiving process starts before the
        # web modules are loaded, extremely early events can't be delivered.
        if self.sio_server:
            # Avoid sending events if there aren't fields the downstream cares about.
            update_data_by_uuid = {uuid: task
                                   for uuid, task in slim_tasks_by_uuid(new_data_by_task_uuid).items()
                                   if task}
            if update_data_by_uuid:
                self.sio_server.emit('tasks-update', update_data_by_uuid)

    def dump_initial_metadata(self):
        self.model_dumper.dump_metadata(self.run_metadata, root_complete=False, flame_complete=False)

    def dump_complete_data_model(self, event_aggregator):
        self.model_dumper.dump_aggregator_complete_data_model(event_aggregator, self.run_metadata,
                                                              self.extra_task_representations)
