import logging

from firex_flame.event_aggregator import slim_tasks_by_uuid
from firex_flame.model_dumper import FlameModelDumper
from firex_flame.flame_helper import query_partial_tasks, get_dict_json_md5

logger = logging.getLogger(__name__)


class FlameAppController:

    def __init__(self, run_metadata, extra_task_representations):

        self.run_metadata = run_metadata
        self.model_dumper = FlameModelDumper(firex_logs_dir=self.run_metadata['logs_dir'])
        self.extra_task_representations = extra_task_representations
        self.listening_query_config_hashes_to_sids = {}

        self.sio_server = None  # Set after creation.

    def send_sio_event(self, new_data_by_task_uuid, tasks_by_uuid):
        # sio_server can be lazy initialized. Since the event receiving process starts before the
        # web modules are loaded, extremely early events can't be delivered.
        if self.sio_server:
            # Avoid sending events if there aren't fields the downstream cares about.
            update_data_by_uuid = {uuid: task
                                   for uuid, task in slim_tasks_by_uuid(new_data_by_task_uuid).items()
                                   if task}
            if update_data_by_uuid:
                self.sio_server.emit('tasks-update', update_data_by_uuid)

            for query_hash, query_and_clients in self.listening_query_config_hashes_to_sids.items():
                # Only bother filtering/emitting the event if there are clients for this query.
                if query_and_clients['client_sids']:
                    # FIXME: if the new data contains additional children,
                    queried_task_data = query_partial_tasks(new_data_by_task_uuid.keys(),
                                                            query_and_clients['query_config'],
                                                            tasks_by_uuid)
                    if queried_task_data:
                        self.sio_server.emit('tasks-query-update', data=queried_task_data, room=query_hash)

    def dump_initial_metadata(self):
        self.model_dumper.dump_metadata(self.run_metadata, root_complete=False, flame_complete=False)

    def dump_complete_data_model(self, event_aggregator):
        self.model_dumper.dump_aggregator_complete_data_model(event_aggregator, self.run_metadata,
                                                              self.extra_task_representations)

    def add_client_task_query_config(self, sid, query_config):
        config_md5 = get_dict_json_md5(query_config)
        if config_md5 not in self.listening_query_config_hashes_to_sids:
            self.listening_query_config_hashes_to_sids[config_md5] = {
                'query_config': query_config,
                'client_sids': set(),
            }
        self.listening_query_config_hashes_to_sids[config_md5]['client_sids'].add(sid)
        self.sio_server.enter_room(sid, room=config_md5)

    def remove_client_task_query(self, sid):
        for config_md5, config in self.listening_query_config_hashes_to_sids.items():
            if sid in config['client_sids']:
                config['client_sids'].remove(sid)
                self.sio_server.leave_room(sid, room=config_md5)
