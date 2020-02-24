import json
import logging
import os
from pathlib import Path
import psutil
import time
import signal
import sys


logger = logging.getLogger(__name__)

DEFAULT_FLAME_TIMEOUT = 60 * 60 * 24 * 2


def get_flame_debug_dir(root_logs_dir):
    return os.path.join(root_logs_dir, 'debug', 'flame')


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

    def __init__(self, broker_url, max_retry_attempts, receiver_ready_file, terminate_on_complete):
        self.broker_url = broker_url
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
        optional_keys = {k[1:] for k in criteria_val.keys() if k.startswith('?')}

        present_required_keys = required_keys.intersection(task.keys())
        if len(required_keys) != len(present_required_keys):
            return False

        present_optional_keys = optional_keys.intersection(task.keys())

        for k in present_required_keys.union(present_optional_keys):
            if task[k] != criteria_val[k]:
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


def _get_descendants_for_criteria(select_paths, descendant_criteria, ancestor_uuid, all_tasks_by_uuid):
    ancestor_descendants_by_uuid = _get_descendants(ancestor_uuid, all_tasks_by_uuid)
    matched_descendants_by_uuid = {}
    for criteria in descendant_criteria:
        for uuid, descendant in ancestor_descendants_by_uuid.items():
            if task_matches_criteria(descendant, criteria):
                # Need no_descendants=True to prevent infinite loops.
                # The fields that are selected for each descendant are determined by all queries, except
                # descendant descendants are never included.
                matched_descendants_by_uuid[uuid] = select_from_task(
                    select_paths,
                    [],  # Never include descendants in descendant queries to avoid infinite loop.
                    descendant,
                    all_tasks_by_uuid)

    return matched_descendants_by_uuid


def select_from_task(select_paths, select_descendants, task, all_tasks_by_uuid):
    selected_dict = {}
    paths_update_dict = _get_paths_from_task(select_paths, task)
    selected_dict.update(paths_update_dict)

    selected_descendants_by_uuid = _get_descendants_for_criteria(select_paths, select_descendants, task['uuid'],
                                                                 all_tasks_by_uuid)
    if selected_descendants_by_uuid:
        selected_dict.update({'descendants': selected_descendants_by_uuid})

    return selected_dict


def flatten(l):
    return [item for sublist in l for item in sublist]


def select_data_for_matches(task, task_queries, all_tasks_by_uuid):
    result_task = {}
    always_select_fields = flatten([q.get('selectPaths', []) for q in task_queries
                                    if q['matchCriteria']['type'] == 'always-select-fields'])
    for query in task_queries:
        is_match = task_matches_criteria(task, query['matchCriteria'])
        if is_match:
            update_dict = select_from_task(always_select_fields + query.get('selectPaths', []),
                                           query.get('selectDescendants', []),
                                           task,
                                           all_tasks_by_uuid,
                                           )
            result_task = deep_merge(result_task, update_dict)
    return result_task


def query_tasks(tasks_by_uuid_to_query, task_queries, all_tasks_by_uuid):
    if not _validate_task_queries(task_queries):
        return {}

    result_tasks_by_uuid = {}
    for id, task in tasks_by_uuid_to_query.items():
        result_task = select_data_for_matches(task, task_queries, all_tasks_by_uuid)
        if result_task:
            result_tasks_by_uuid[id] = result_task

    return result_tasks_by_uuid
