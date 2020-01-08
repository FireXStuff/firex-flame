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


def find(keys, input_dict):
    result = input_dict
    for key in keys:
        try:
            result = result[key]
        except KeyError:
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
