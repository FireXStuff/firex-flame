import json
import logging
import os
from pathlib import Path
import psutil
import time
import threading
from socket import gethostname
import signal
import sys

import requests

logger = logging.getLogger(__name__)

DEFAULT_FLAME_TIMEOUT = 60 * 60 * 24 * 2


def get_flame_debug_dir(root_logs_dir):
    return os.path.join(root_logs_dir, 'debug', 'flame_server')


def get_flame_pid_file_path(root_logs_dir):
    return os.path.join(get_flame_debug_dir(root_logs_dir), 'flame.pid')


def get_flame_pid(root_logs_dir):
    return int(Path(get_flame_pid_file_path(root_logs_dir)).read_text().strip())


def wait_until(predicate, timeout, sleep_for, *args, **kwargs):
    max_time = time.time() + timeout
    while time.time() < max_time:
        if predicate(*args, **kwargs):
            return True
        time.sleep(sleep_for)
    return predicate(*args, **kwargs)


def wait_until_pid_not_exist(pid, timeout=7, sleep_for=1):
    return wait_until(lambda p: not psutil.pid_exists(p), timeout, sleep_for, pid)


def wait_until_web_request_ok(url, timeout=10, sleep_for=1):
    def try_request_get(url):
        try:
            return requests.get(url).ok
        except requests.exceptions.ConnectionError:
            return False

    return wait_until(try_request_get, timeout, sleep_for, url)


def wait_until_path_exist(path, timeout=7, sleep_for=1):
    return wait_until(os.path.exists, timeout, sleep_for, path)


def json_file_predicate(json_file_path, pred):
    if not os.path.isfile(json_file_path):
        return False
    try:
        file_data = json.loads(Path(json_file_path).read_text())
    except (json.decoder.JSONDecodeError, OSError):
        return False
    else:
        return pred(file_data)


def _interrupt_main_thread():
    try:
        import _thread as thread
    except ImportError:
        # noinspection PyUnresolvedReferences
        import thread
    logger.info('Interrupting main thread.')
    thread.interrupt_main()


def stop_main_thread(reason):
    logging.info("Stopping entire Flame Server for reason: %s" % reason)
    if threading.current_thread() is threading.main_thread():
        logger.info('Stopped from main thread, will sysexit.')
        sys.exit(0)
    else:
        _interrupt_main_thread()


def get_rec_file(log_dir):
    return os.path.join(log_dir, 'flame.rec')


def get_flame_url(port, hostname=gethostname()):
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
