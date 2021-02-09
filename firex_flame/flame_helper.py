import json
import logging
import os
from pathlib import Path
import psutil
import time
import signal
from collections import namedtuple

from firexapp.events.model import ADDITIONAL_CHILDREN_KEY
from firexapp.submit.uid import Uid

logger = logging.getLogger(__name__)

DEFAULT_FLAME_TIMEOUT = 60 * 60 * 24 * 2

# This structure contains an index by UUID for both ancestors and descendants. This is memory inefficient,
# but makes queries that would involve multiple graph traversals very fast.
# TODO: If further performance enhancements are sought, this structure could be maintained during event receiving
#  so that it isn't re-calculated per task query.
FlameTaskGraph = namedtuple('FlameTaskGraph', ['tasks_by_uuid', 'ancestors_by_uuid', 'descendants_by_uuid'])


def get_flame_redirect_file_path(root_logs_dir):
    return os.path.join(root_logs_dir, 'flame.html')


def get_flame_debug_dir(root_logs_dir):
    return os.path.join(root_logs_dir, Uid.debug_dirname, 'flame')


def get_flame_pid_file_path(root_logs_dir):
    return os.path.join(get_flame_debug_dir(root_logs_dir), 'flame.pid')


def get_flame_pid(root_logs_dir):
    return int(Path(get_flame_pid_file_path(root_logs_dir)).read_text().strip())


def wait_until(predicate, timeout, sleep_for, *args, **kwargs):
    max_time = time.time() + timeout
    while time.time() < max_time:
        pred_result = predicate(*args, **kwargs)
        if pred_result:
            return pred_result
        time.sleep(sleep_for)
    return predicate(*args, **kwargs)


def wait_until_pid_not_exist(pid, timeout=7, sleep_for=1):
    return wait_until(lambda p: not psutil.pid_exists(p), timeout, sleep_for, pid)


def web_request_ok(url):
    import requests
    try:
        return requests.get(url).ok
    except requests.exceptions.ConnectionError:
        return False


def wait_until_web_request_ok(url, timeout=10, sleep_for=1):
    return wait_until(web_request_ok, timeout, sleep_for, url)


def wait_until_path_exist(path, timeout=7, sleep_for=1):
    return wait_until(os.path.exists, timeout, sleep_for, path)


def json_file_fn(json_file_path, fn):
    if not os.path.isfile(json_file_path):
        return False
    try:
        file_data = json.loads(Path(json_file_path).read_text())
    except (json.decoder.JSONDecodeError, OSError):
        return False
    else:
        return fn(file_data)


def get_rec_file(log_dir):
    return os.path.join(get_flame_debug_dir(log_dir), 'flame.rec')


def find_rec_file(log_dir):
    # Formerly was used for backwards compatability, now an alias for get_rec_file
    return get_rec_file(log_dir)


def get_flame_url(port, hostname=None):
    if hostname is None:
        from socket import gethostname
        hostname = gethostname()
    return 'http://%s:%d' % (hostname, int(port))


class PathNotFoundException(Exception):
    pass


def find(keys, input_dict, raise_error=False):
    result = input_dict
    for key in keys:
        try:
            result = result[key]
        except Exception:
            if raise_error:
                raise PathNotFoundException()
            return None
    return result


def filter_paths(input_dict, paths_to_values):
    results = {}
    for in_key, in_vals in input_dict.items():
        results[in_key] = []
        for in_val in in_vals:
            matches_all = all(to_equal == find(p, in_val) for p, to_equal in paths_to_values.items())
            if matches_all:
                results[in_key].append(in_val)
    return results


def kill_flame(log_dir, sig=signal.SIGKILL, timeout=10):
    flame_pid = get_flame_pid(log_dir)
    kill_and_wait(flame_pid, sig, timeout)
    return flame_pid


def kill_and_wait(pid, sig=signal.SIGKILL, timeout=10):
    if psutil.pid_exists(pid):
        os.kill(pid, sig)
        wait_until_pid_not_exist(pid, timeout=timeout)
    return not psutil.pid_exists(pid)


def create_rel_symlink(existing_path, symlink, target_is_directory=False):
    rel_new_file = os.path.relpath(existing_path, start=os.path.dirname(symlink))
    os.symlink(rel_new_file, symlink, target_is_directory=target_is_directory)


class BrokerConsumerConfig:

    def __init__(self, max_retry_attempts, receiver_ready_file, terminate_on_complete):
        self.max_retry_attempts = max_retry_attempts
        self.receiver_ready_file = receiver_ready_file
        self.terminate_on_complete = terminate_on_complete


def is_json_file(file_path):
    try:
        json.loads(Path(file_path).read_text())
    except json.decoder.JSONDecodeError:
        return False
    else:
        return True


def _both_instance(o1, o2, _type):
    return isinstance(o1, _type) and isinstance(o2, _type)


def deep_merge(dict1, dict2):
    result = dict(dict1)
    for d2_key in dict2:
        if d2_key in dict1:
            v1 = dict1[d2_key]
            v2 = dict2[d2_key]
            if _both_instance(v1, v2, dict):
                result[d2_key] = deep_merge(v1, v2)
            elif _both_instance(v1, v2, list):
                result[d2_key] = v1 + v2
            elif _both_instance(v1, v2, set):
                result[d2_key] = v1.union(v2)
            elif v1 == v2:
                # already the same value in both dicts, take from either.
                result[d2_key] = v1
            else:
                # Both d1 and d2 have entries for d2_key, both entries are not dicts or lists or sets,
                # and the values are not the same. This is a conflict.
                # Overwrite d1's value to simulate dict.update() behaviour.
                result[d2_key] = v2
        else:
            # New key for d1, just add it.
            result[d2_key] = dict2[d2_key]
    return result


def _validate_task_queries(task_representation):
    if not isinstance(task_representation, list):
        return False

    missing_criterias = [r for r in task_representation
                         if 'matchCriteria' not in r or not isinstance(r['matchCriteria'], dict)]
    if missing_criterias:
        return False

    # TODO: validate matchCriteria themselves

    return True


def _normalize_criteria_key(k):
    return k[1:] if k.startswith('?') else k


def task_matches_criteria(task: dict, criteria: dict):
    if criteria['type'] == 'all':
        return True

    if criteria['type'] == 'always-select-fields':
        # always-select-fields doesn't cause matches (tasks to be included), but paths here are always included
        # in results.
        return False

    if criteria['type'] == 'equals':
        criteria_val = criteria['value']
        # TODO: if more adjusting qualifiers are added, this needs to be reworked.
        required_keys = {k for k in criteria_val.keys() if not k.startswith('?')}
        optional_keys = {_normalize_criteria_key(k) for k in criteria_val.keys() if k.startswith('?')}

        present_required_keys = required_keys.intersection(task.keys())
        if len(required_keys) != len(present_required_keys):
            return False

        present_optional_keys = optional_keys.intersection(task.keys())
        normalized_criteria = {_normalize_criteria_key(k): v for k, v in criteria_val.items()}
        for k in present_required_keys.union(present_optional_keys):
            if task[k] != normalized_criteria[k]:
                return False

        return True

    return False


def _create_dict_with_path_val(path_list, val):
    r = {}
    lastest_dict = r
    for i, e in enumerate(path_list):
        is_last = i == len(path_list) - 1
        if is_last:
            lastest_dict[e] = val
        else:
            lastest_dict[e] = {}
            lastest_dict = lastest_dict[e]
    return r


def _get_paths_from_task(paths, task):
    r = {}
    for path in paths:
        try:
            path_list = path.split('.')
            val = find(path_list, task, raise_error=True)
        except PathNotFoundException:
            # Don't update the results dict if the current task doesn't have the path.
            pass
        else:
            r = deep_merge(r, _create_dict_with_path_val(path_list, val))
    return r


def _get_child_tasks_by_uuid(parent_uuid, all_tasks_by_uuid):
    return {u: t for u, t in all_tasks_by_uuid.items() if t['parent_id'] == parent_uuid}


def _get_descendants(uuid, all_tasks_by_uuid):
    descendants_by_uuid = _get_child_tasks_by_uuid(uuid, all_tasks_by_uuid)
    uuids_to_check = list(descendants_by_uuid.keys())
    while uuids_to_check:
        cur_descendant_uuid = uuids_to_check.pop()
        cur_descendant_children_by_uuid = _get_child_tasks_by_uuid(cur_descendant_uuid, all_tasks_by_uuid)
        descendants_by_uuid.update(cur_descendant_children_by_uuid)
        uuids_to_check += list(cur_descendant_children_by_uuid.keys())

    return descendants_by_uuid


def _get_descendants_for_criteria(select_paths, descendant_criteria, ancestor_uuid, task_graph: FlameTaskGraph):
    ancestor_descendants = task_graph.descendants_by_uuid[ancestor_uuid]
    matched_descendants_by_uuid = {}
    for criteria in descendant_criteria:
        for descendant in ancestor_descendants:
            if task_matches_criteria(descendant, criteria):
                # Need no_descendants=True to prevent infinite loops.
                # The fields that are selected for each descendant are determined by all queries, except
                # descendant descendants are never included.
                matched_descendants_by_uuid[descendant['uuid']] = select_from_task(
                    select_paths,
                    [],  # Never include descendants in descendant queries to avoid infinite loop.
                    descendant,
                    task_graph)

    return matched_descendants_by_uuid


def select_from_task(select_paths, select_descendants, task, task_graph: FlameTaskGraph):
    selected_dict = {}
    paths_update_dict = _get_paths_from_task(select_paths, task)
    selected_dict.update(paths_update_dict)

    selected_descendants_by_uuid = _get_descendants_for_criteria(select_paths, select_descendants, task['uuid'],
                                                                 task_graph)
    if selected_descendants_by_uuid:
        selected_dict.update({'descendants': selected_descendants_by_uuid})

    return selected_dict


def flatten(l):
    return [item for sublist in l for item in sublist]


def get_always_select_fields(task_queries):
    return flatten([q.get('selectPaths', []) for q in task_queries
                    if q['matchCriteria']['type'] == 'always-select-fields'])


def select_ancestor_of_task_descendant_match(uuid, query, select_paths, task_graph: FlameTaskGraph):
    # Should the current task be included in the result because it matches some descendant criteria?
    task = task_graph.tasks_by_uuid[uuid]
    matching_criteria = [criteria for criteria in query.get('selectDescendants', [])
                         if task_matches_criteria(task, criteria)]
    if matching_criteria:
        # The current task matches some descendant criteria. Confirm that some ancestor matches the top-level
        # criteria.
        ancestor = next((a for a in task_graph.ancestors_by_uuid[uuid]
                         if task_matches_criteria(a, query['matchCriteria'])), None)
        if ancestor:
            # The current task and its ancestor should be included in the result.
            return ancestor['uuid'], select_from_task(select_paths, matching_criteria, ancestor, task_graph)
    return None, {}


def _get_children_by_uuid(tasks_by_uuid):
    children_by_uuid = {}
    for u, t in tasks_by_uuid.items():
        if u not in children_by_uuid:
            # Ensure every UUID has an entry in the result, even UUIDs with no children.
            children_by_uuid[u] = []

        # TODO: consider handling tasks with no 'parent_id' differently from tasks with None 'parent_id',
        #   since the latter case is the root task and the former seems inexplicable.
        parent_id = t.get('parent_id')
        if parent_id is not None:
            if parent_id not in children_by_uuid:
                children_by_uuid[parent_id] = []
            children_by_uuid[parent_id].append(t)

    return children_by_uuid


def _create_task_graph(tasks_by_uuid):
    children_by_uuid = _get_children_by_uuid(tasks_by_uuid)
    descendant_uuids_by_uuid = {}
    ancestor_uuids_by_uuid = {}
    root_task = next((t for t in tasks_by_uuid.values() if t['parent_id'] is None), None)
    if root_task:
        tasks_to_check = [root_task]
        while tasks_to_check:
            cur_task = tasks_to_check.pop()

            if cur_task['uuid'] not in ancestor_uuids_by_uuid:
                ancestor_uuids_by_uuid[cur_task['uuid']] = set()
            cur_task_ancestor_uuids = ancestor_uuids_by_uuid[cur_task['uuid']]

            # The task tree is being walked top-down, so it's safe to expect ancestors to be populated.
            if cur_task.get('parent_id') is not None and cur_task['parent_id'] in ancestor_uuids_by_uuid:
                # This task's ancestors are its parent's ancestors plus its parent.
                ancestor_uuids = ancestor_uuids_by_uuid[cur_task['parent_id']].union([cur_task['parent_id']])
                cur_task_ancestor_uuids.update(ancestor_uuids)

            # Update ancestors of additional children.
            additional_children_uuids = cur_task.get(ADDITIONAL_CHILDREN_KEY, [])
            for additional_child_uuid in additional_children_uuids:
                if additional_child_uuid not in ancestor_uuids_by_uuid:
                    ancestor_uuids_by_uuid[additional_child_uuid] = set()
                ancestor_uuids_by_uuid[additional_child_uuid].update(cur_task_ancestor_uuids)

            descendant_uuids_by_uuid[cur_task['uuid']] = set(additional_children_uuids)
            for ancestor_uuid in cur_task_ancestor_uuids:
                descendant_uuids_by_uuid[ancestor_uuid].add(cur_task['uuid'])
                descendant_uuids_by_uuid[ancestor_uuid].update(additional_children_uuids)

            # traverse the graph via real children only, not additional_children.
            tasks_to_check.extend(children_by_uuid[cur_task['uuid']])

    ancestors_by_uuid = {u: [tasks_by_uuid[au] for au in ancestor_uuids if au in tasks_by_uuid]
                         for u, ancestor_uuids in ancestor_uuids_by_uuid.items()}
    descendants_by_uuid = {u: [tasks_by_uuid[du] for du in descendant_uuids if du in tasks_by_uuid]
                           for u, descendant_uuids in descendant_uuids_by_uuid.items()}
    return FlameTaskGraph(tasks_by_uuid, ancestors_by_uuid, descendants_by_uuid)


def select_data_for_matches(task_uuid, task_queries, task_graph: FlameTaskGraph, match_descendant_criteria):
    result_tasks_by_uuid = {}
    always_select_fields = get_always_select_fields(task_queries)
    for query in task_queries:
        task = task_graph.tasks_by_uuid[task_uuid]
        matches_criteria = task_matches_criteria(task, query['matchCriteria'])
        select_paths = always_select_fields + query.get('selectPaths', [])
        updates_by_uuid = {}
        if matches_criteria:
            updates_by_uuid[task_uuid] = select_from_task(select_paths, query.get('selectDescendants', []), task,
                                                          task_graph)

        if match_descendant_criteria:
            uuid, task_update = select_ancestor_of_task_descendant_match(task_uuid, query, select_paths, task_graph)
            if uuid:
                updates_by_uuid[uuid] = task_update

        if updates_by_uuid:
            result_tasks_by_uuid = deep_merge(result_tasks_by_uuid, updates_by_uuid)

    return result_tasks_by_uuid


def _query_flame_tasks(task_uuids_to_query, task_queries, all_tasks_by_uuid, match_descendant_criteria):
    if not _validate_task_queries(task_queries):
        return {}

    task_graph = _create_task_graph(all_tasks_by_uuid)
    result_tasks_by_uuid = {}
    for uuid in task_uuids_to_query:
        selected_tasks_by_uuid = select_data_for_matches(uuid, task_queries, task_graph, match_descendant_criteria)
        result_tasks_by_uuid = deep_merge(result_tasks_by_uuid, selected_tasks_by_uuid)

    return result_tasks_by_uuid


def query_full_tasks(all_tasks_by_uuid, task_queries):
    # When querying a full set of tasks, descendants will be included when their ancestors are matched.
    return _query_flame_tasks(all_tasks_by_uuid.keys(), task_queries, all_tasks_by_uuid,
                              match_descendant_criteria=False)


def query_partial_tasks(task_uuids_to_query, task_queries, all_tasks_by_uuid):
    # When querying a partial set of tasks, count descendants as matches to be included in the result.
    return _query_flame_tasks(task_uuids_to_query, task_queries, all_tasks_by_uuid, match_descendant_criteria=True)


def get_dict_json_md5(query_config):
    import hashlib
    return hashlib.md5(json.dumps(query_config, sort_keys=True).encode('utf-8')).hexdigest()
