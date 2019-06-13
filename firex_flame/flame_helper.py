import os
from pathlib import Path

def get_flame_debug_dir(root_logs_dir):
    return os.path.join(root_logs_dir, 'debug', 'flame_server')


def get_flame_pid_file_path(root_logs_dir):
    return os.path.join(get_flame_debug_dir(root_logs_dir), 'flame.pid')


def get_flame_pid(root_logs_dir):
    return int(Path(get_flame_pid_file_path(root_logs_dir)).read_text().strip())
