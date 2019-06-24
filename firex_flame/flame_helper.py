import logging
import os
from pathlib import Path
import psutil
import time
import threading
from socket import gethostname
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


def _interrupt_main_thread():
    try:
        import _thread as thread
    except ImportError:
        # noinspection PyUnresolvedReferences
        import thread
    logger.info('Exiting main thread')
    thread.interrupt_main()


def stop_main_thread(reason):
    logging.info("Stopping for reason: %s" % reason)
    if threading.current_thread() is threading.main_thread():
        logger.info('sysexit from main thread')
        sys.exit(0)
    else:
        _interrupt_main_thread()


def get_rec_file(log_dir):
    return os.path.join(log_dir, 'flame2.rec')


def get_flame_url(port, hostname=gethostname()):
    return 'http://%s:%d' % (hostname, int(port))
