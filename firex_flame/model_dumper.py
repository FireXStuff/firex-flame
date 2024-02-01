import json
import logging
import os
from pathlib import Path
import tarfile
import tempfile

from gevent.fileobject import FileObject

from firex_flame.event_aggregator import slim_tasks_by_uuid, COMPLETE_STATES
from firex_flame.flame_helper import get_flame_debug_dir, FlameTaskGraph, \
    convert_json_paths_in_query

logger = logging.getLogger(__name__)


def _atomic_write_json(filename, data):
    filename = os.path.realpath(filename)
    filedir = os.path.dirname(filename)
    with tempfile.NamedTemporaryFile(mode='w',
                                     dir=filedir,
                                     delete=False) as f:
        json.dump(
            data,
            fp=FileObject(f, 'w'), # closes fd
            sort_keys=True,
            indent=2)

        os.chmod(f.name, 0o664)
        os.replace(f.name, filename)


def get_flame_model_dir(firex_logs_dir):
    return os.path.join(get_flame_debug_dir(firex_logs_dir), 'model')


def get_flame_old_model_dir(firex_logs_dir):
    return os.path.join(firex_logs_dir, 'flame_model')


# Temporarily support both current and old model directory locations.
def find_flame_model_dir(firex_logs_dir):
    model_dir = get_flame_model_dir(firex_logs_dir)
    if os.path.isdir(model_dir):
        return model_dir
    return get_flame_old_model_dir(firex_logs_dir)


def _get_base_model_dir(firex_logs_dir=None, root_model_dir=None):
    assert bool(firex_logs_dir) ^ bool(root_model_dir), \
        "Need exclusively either logs dir or root model dir."
    if firex_logs_dir:
        return find_flame_model_dir(firex_logs_dir)
    else:
        return root_model_dir


def get_all_tasks_dir(firex_logs_dir=None, root_model_dir=None):
    return os.path.join(_get_base_model_dir(firex_logs_dir, root_model_dir), 'full-tasks')


def get_run_metadata_file(firex_logs_dir=None, root_model_dir=None):
    return os.path.join(_get_base_model_dir(firex_logs_dir, root_model_dir), 'run-metadata.json')


def get_run_metadata(firex_logs_dir=None, root_model_dir=None):
    run_metadata_file = get_run_metadata_file(firex_logs_dir, root_model_dir)
    if os.path.isfile(run_metadata_file):
        return json.loads(Path(run_metadata_file).read_text())
    return None


def get_flame_url(firex_logs_dir=None, root_model_dir=None):
    return get_run_metadata(firex_logs_dir, root_model_dir).get('flame_url', None)


def get_tasks_slim_file(firex_logs_dir=None, root_model_dir=None):
    return os.path.join(_get_base_model_dir(firex_logs_dir, root_model_dir), 'slim-tasks.json')


def get_model_complete_file(firex_logs_dir=None, root_model_dir=None):
    return os.path.join(_get_base_model_dir(firex_logs_dir, root_model_dir), 'model-complete')


def load_slim_tasks(log_dir):
    return json.loads(Path(get_tasks_slim_file(log_dir)).read_text())


def socketio_client_task_queries_request(flame_url, task_queries):
    import socketio
    sio_client = socketio.Client()
    sio_client.connect(flame_url)
    try:
        resp = {'tasks_by_uuid': None}
        @sio_client.on('graph-state')
        def my_event(data):
            resp['tasks_by_uuid'] = data

        sio_client.call('send-graph-state', {'task_queries': task_queries})
        assert resp['tasks_by_uuid'] is not None
    finally:
        sio_client.disconnect()

    return resp['tasks_by_uuid']


def load_task_representation(firex_logs_dir, representation_file, consider_running=False):
    with open(representation_file) as fp:
            representation_data = json.load(fp)

    task_rep_file = os.path.join(_get_base_model_dir(firex_logs_dir=firex_logs_dir),
                                 representation_data['model_file_name'])
    # TODO: this assumes the task representation file is only present once it's complete!
    #   If dumping is moved to occur during event reception, this will need to consider run_metadata.is_complete.
    if os.path.isfile(task_rep_file):
        # If file has already been dumped, just load it.
        with open(task_rep_file) as fp:
            return json.load(fp)
    elif consider_running:
        # If file hasn't been dumped, try to query a running flame server.
        run_metadata_file = get_run_metadata_file(firex_logs_dir=firex_logs_dir)
        if os.path.isfile(run_metadata_file):
            with open(run_metadata_file) as fp:
                run_metadata = json.load(fp)
            return socketio_client_task_queries_request(run_metadata['flame_url'],
                                                        representation_data['task_queries'])

    return None


def get_full_task_path(logs_dir, task_uuid):
    return os.path.join(get_all_tasks_dir(firex_logs_dir=logs_dir), task_uuid + '.json')


def load_full_task(log_dir, task_uuid):
    return json.loads(Path(get_full_task_path(log_dir, task_uuid)).read_text())


def index_tasks_by_names(tasks, names):
    tasks_by_name = {n: [] for n in names}
    for t in tasks:
        if t['name'] in names:
            tasks_by_name[t['name']].append(t)
    return tasks_by_name


def get_model_full_tasks_by_names(log_dir, task_names):
    if isinstance(task_names, str):
        task_names = [task_names]
    tasks_by_uuid = get_full_tasks_by_slim_pred(log_dir, lambda st: st.get('name', None) in task_names)
    return index_tasks_by_names(tasks_by_uuid.values(), task_names)


def get_full_tasks_by_slim_pred(log_dir, slim_predicate):
    return {uuid: load_full_task(log_dir, uuid)
            for uuid, slim_task in load_slim_tasks(log_dir).items()
            if slim_predicate(slim_task)}


def is_dump_complete(firex_logs_dir):
    return os.path.exists(get_model_complete_file(firex_logs_dir=firex_logs_dir))


class FlameModelDumper:

    def __init__(self,
                 task_graph: FlameTaskGraph,
                 firex_logs_dir=None, root_model_dir=None):
        assert bool(firex_logs_dir) ^ bool(root_model_dir), \
            "Dumper needs exclusively either logs dir or root model dir."
        self.task_graph = task_graph
        if firex_logs_dir:
            self.root_model_dir = get_flame_model_dir(firex_logs_dir)
        else:
            self.root_model_dir = root_model_dir
        os.makedirs(self.root_model_dir, exist_ok=True)

        self.full_tasks_dir = get_all_tasks_dir(root_model_dir=self.root_model_dir)
        os.makedirs(self.full_tasks_dir, exist_ok=True)
        self.slim_tasks_file = get_tasks_slim_file(root_model_dir=self.root_model_dir)

    def dump_metadata(self, run_metadata, root_complete, flame_complete):
        metadata_model_file = get_run_metadata_file(root_model_dir=self.root_model_dir)
        complete = {'run_complete': root_complete, 'flame_recv_complete': flame_complete}
        _atomic_write_json(metadata_model_file, run_metadata | complete)
        return metadata_model_file

    def dump_aggregator_complete_data_model(self, run_metadata=None, extra_task_representations=tuple(),
                                            dump_task_jsons=True):
        self.dump_complete_data_model(run_metadata, dump_task_jsons)
        for repr_file_path in extra_task_representations:
            self.dump_task_representation(repr_file_path)
        Path(get_model_complete_file(root_model_dir=self.root_model_dir)).touch()

    def dump_full_task(self, uuid, task):
        _atomic_write_json(os.path.join(self.full_tasks_dir, f'{uuid}.json'), task)

    def dump_slim_tasks(self):
        _atomic_write_json(
            self.slim_tasks_file,
            slim_tasks_by_uuid(self.task_graph.tasks_by_uuid))

    def dump_complete_data_model(self, run_metadata=None, dump_task_jsons=True):
        logger.info("Starting to dump complete Flame model.")

        if dump_task_jsons:
            # Write JSON file with minimum amount of info to render graph.
            self.dump_slim_tasks()

            # Write one JSON file per task.
            for uuid, task in self.task_graph.tasks_by_uuid.items():
                self.dump_full_task(uuid, task)

        paths_to_compress = [self.slim_tasks_file, self.full_tasks_dir]
        if run_metadata:
            # Write metadata file.
            # Note that since a flame can terminate (e.g. via timeout) before a run, there is no guarantee
            # that the run_metadata model file will ever have root_complete: true.
            root_uuid = self.task_graph.root_uuid
            root_complete = self.task_graph.tasks_by_uuid.get(root_uuid, {'state': None})['state'] in COMPLETE_STATES
            run_metadata_with_root = {**run_metadata, 'root_uuid': root_uuid}
            metadata_model_file = self.dump_metadata(run_metadata_with_root, root_complete, flame_complete=True)
            paths_to_compress.append(metadata_model_file)

        # TODO: use shutil.which to find out of there is a native tar command, and use that when present to speedup
        #  shutdown.
        # Write a tar.gz file containing all the files dumped above.
        with tarfile.open(os.path.join(self.root_model_dir, 'full-run-state.tar.gz'), "w:gz") as tar:
            for path in paths_to_compress:
                tar.add(path, arcname=os.path.basename(path))

        logger.info("Finished dumping complete Flame model.")

    def dump_task_representation(self, representation_file):
        logger.info(f"Starting to dump task representation of: {representation_file}.")

        try:
            representation_data = load_tasks_representation(representation_file)
            out_file = os.path.join(self.root_model_dir, representation_data['model_file_name'])
            queried_tasks = self.task_graph.query_full_tasks(
                convert_json_paths_in_query(representation_data['task_queries'])
            )
            _atomic_write_json(out_file, queried_tasks)
        except Exception as ex:
            # Don't interfere with shutdown even if extra representation dumping fails.
            logger.error(f"Failed to dump representation of {representation_file}.")
            logger.exception(ex)
        else:
            logger.info(f"Finished dumping {len(queried_tasks)} task representation of {representation_file} to {out_file}.")

def load_tasks_representation(rep_file):
    with open(rep_file, encoding='utf-8') as fp:
        return json.load(fp)

def load_tasks_representation_task_queries(rep_file):
    return load_tasks_representation(rep_file)['task_queries']