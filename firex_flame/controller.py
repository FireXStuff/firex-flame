import logging
from typing import Any, Optional
import time

from firex_flame.event_aggregator import slim_tasks_by_uuid
from firex_flame.model_dumper import FlameModelDumper, load_tasks_representation_task_queries
from firex_flame.flame_helper import get_dict_json_md5, REVOKE_REASON_KEY, \
    REVOKE_TIMESTAMP_KEY, FlameTaskGraph, convert_json_paths_in_query
import socketio


logger = logging.getLogger(__name__)


def _send_listening_query_sio_event(
    sio_server,
    new_data_by_task_uuid,
    task_graph: FlameTaskGraph,
    listening_query_config_hashes_to_sids,
):
    for query_hash, query_and_clients in listening_query_config_hashes_to_sids.items():
        # Only bother filtering/emitting the event if there are clients for this query.
        if query_and_clients['client_sids']:
            # FIXME: if the new data contains additional children,
            queried_task_data = task_graph.query_partial_tasks(
                new_data_by_task_uuid.keys(),
                query_and_clients['query_config'],
            )
            if queried_task_data:
                sio_server.emit('tasks-query-update', data=queried_task_data, room=query_hash)


class FlameAppController:

    def __init__(
        self,
        run_metadata: dict[str, Any],
        extra_task_representations,
        tasks_by_uuid, # FIXME: Note this instance is relevant, not just the data
    ):
        self.run_metadata = run_metadata
        self.graph : FlameTaskGraph = FlameTaskGraph(tasks_by_uuid)
        self.model_dumper = FlameModelDumper(self.graph, firex_logs_dir=self.run_metadata['logs_dir'])
        self.extra_task_representations = extra_task_representations
        self.listening_query_config_hashes_to_sids = {}

        self.sio_server : Optional[socketio.Server] = None  # Set after creation.


    def update_graph_and_sio_clients(self, new_data_by_task_uuid):
        self.graph.update_graph_from_celery_events(new_data_by_task_uuid.values())
        slim_update_data_by_uuid = {uuid: task
                                    for uuid, task in slim_tasks_by_uuid(new_data_by_task_uuid).items()
                                    if task}
        # sio_server can be lazy initialized. Since the event receiving process starts before the
        # web modules are loaded, extremely early events can't be delivered.
        if self.sio_server:
            # Avoid sending events if there aren't fields the downstream cares about.
            if slim_update_data_by_uuid:
                self.sio_server.emit('tasks-update', slim_update_data_by_uuid)

            _send_listening_query_sio_event(
                self.sio_server,
                new_data_by_task_uuid,
                self.graph,
                self.listening_query_config_hashes_to_sids,
            )
        return slim_update_data_by_uuid

    def get_revoke_data(self):
        return {
            k: v for k, v in self.run_metadata.items()
            if k in [REVOKE_REASON_KEY, REVOKE_TIMESTAMP_KEY]
        }

    def update_revoke_reason(self, revoke_reason, revoke_timestap=None):
        if revoke_timestap is None:
            revoke_timestap = time.time()
        self.dump_updated_metadata(
            {
                REVOKE_REASON_KEY: revoke_reason,
                REVOKE_TIMESTAMP_KEY: revoke_timestap,
            }
        )

    def dump_updated_metadata(self, update: dict[str, Any]) -> None:
        self.run_metadata.update(update)
        self.model_dumper.dump_metadata(self.run_metadata, root_complete=False, flame_complete=False)

    def dump_complete_data_model(self):
        self.model_dumper.dump_aggregator_complete_data_model(
            self.run_metadata,
            self.extra_task_representations,
            # Event broker's running_dumper_queue will dump task JSONs, so no need to dump again.
            dump_task_jsons=False,
        )

    def dump_extra_task_representations(self) -> None:
        for repr_file_path in self.extra_task_representations:
            self.model_dumper.dump_task_representation(repr_file_path)

    def dump_full_task(self, uuid, task):
        self.model_dumper.dump_full_task(uuid, task)

    def dump_slim_tasks(self) -> None:
        self.model_dumper.dump_slim_tasks()

    def add_client_task_query_config(self, sid, query_config):
        config_md5 = get_dict_json_md5(query_config)
        if config_md5 not in self.listening_query_config_hashes_to_sids:
            self.listening_query_config_hashes_to_sids[config_md5] = {
                'query_config': convert_json_paths_in_query(query_config),
                'client_sids': set(),
            }
        if sid:
            self.listening_query_config_hashes_to_sids[config_md5]['client_sids'].add(sid)
            self.sio_server.enter_room(sid, room=config_md5)

    def remove_client_task_query(self, sid):
        for config_md5, config in self.listening_query_config_hashes_to_sids.items():
            if sid in config['client_sids']:
                config['client_sids'].remove(sid)
                self.sio_server.leave_room(sid, room=config_md5)

    def get_task(self, uuid):
        return self.graph.get_task(uuid)

    def get_all_task_uuids(self):
        return self.graph.get_all_task_uuids()

    def set_sio_server(self, sio_server):
        self.sio_server = sio_server
        for repr_file in self.extra_task_representations:
            # This caches jsonpath parsing.
            self.add_client_task_query_config(
                sid=None,
                query_config=load_tasks_representation_task_queries(repr_file)
            )

    def query_full_tasks(self, task_queries):
        q_hash = get_dict_json_md5(task_queries)
        if q_hash in self.listening_query_config_hashes_to_sids:
            task_queries = self.listening_query_config_hashes_to_sids[q_hash]['query_config']
        else:
            task_queries = convert_json_paths_in_query(task_queries)
        return self.graph.query_full_tasks(task_queries)