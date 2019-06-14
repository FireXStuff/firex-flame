import os
from pathlib import Path
import psutil
import time

import requests

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
