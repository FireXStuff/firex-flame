import argparse
import eventlet
import logging
import os
from pathlib import Path
import signal
import threading

from firex_flame.main_app import run_flame
from firex_flame.flame_helper import get_flame_debug_dir, get_flame_pid_file_path, DEFAULT_FLAME_TIMEOUT, \
    stop_main_thread, get_flame_url

logger = logging.getLogger(__name__)
eventlet.monkey_patch()


def _parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', help='Port for starting the web server on', type=int, default=8000)
    parser.add_argument('--uid', help='Unique identifier for the represented FireX run.')
    parser.add_argument('--logs_dir', help='Logs directory.', default=None)
    parser.add_argument('--chain', help='Chain of the run.', default=None)
    parser.add_argument('--recording', help='A file containing the recording of celery events.', default=None)
    parser.add_argument('--central_server', help='A central web server from which the UI and logs can be served.',
                        default=None)
    parser.add_argument('--central_server_ui_path', help='Path part of the URL from which the central server serves'
                                                         'the UI. Only has meaning when a central_server is supplied.',
                        default=None)
    # TODO: could validate either rec file exists or broker is supplied.
    parser.add_argument('--broker', help='Celery broker.', default=None)
    parser.add_argument('--flame_timeout', help='Maximum lifetime of this service, in seconds', type=int,
                        default=DEFAULT_FLAME_TIMEOUT)
    parser.add_argument('--broker_recv_ready_file', help='File to create immediately before capturing celery events.',
                        default=None)
    parser.add_argument('--broker_max_retry_attempts',
                        help='Number of retry attempts if connection with broker is lost. '
                             'Retries are backed-off exponentially with base 2,'
                             'so a value of 3 here with cause sleeps between retries of '
                             '1 sec, 2 sec, 4 sec, before giving up on retries. Default waits at least a total of '
                             '63 seconds before giving up on retries.',
                        type=int,
                        default=5)
    return parser.parse_args()


def _sigterm_handler(_, __):
    stop_main_thread('SIGTERM detected, shutting down')


def _sigint_handler(_, __):
    stop_main_thread('SIGINT detected, shutting down')


def _exit_on_timeout():
    stop_main_thread('timeout exceeded')


def _config_logging(root_logs_dir):
    flame_logs_dir = get_flame_debug_dir(root_logs_dir)
    os.makedirs(flame_logs_dir, exist_ok=True)

    Path(get_flame_pid_file_path(root_logs_dir)).write_text(str(os.getpid()))

    logging.basicConfig(
        filename=os.path.join(flame_logs_dir, 'flame.log'),
        format='[%(asctime)s][%(levelname)s][%(name)s]: %(message)s',
        level=logging.DEBUG,
    )
    # This module is very noisy (logs all data sent), so turn up the level.
    logging.getLogger('engineio.server').setLevel(logging.WARNING)


def _create_run_metadata(cli_args):
    return {
        'uid': cli_args.uid,
        'logs_dir': cli_args.logs_dir,
        'central_server': cli_args.central_server,
        'central_server_ui_path': cli_args.central_server_ui_path,
        'chain': cli_args.chain,
        # TODO: externalize.
        'central_documentation_url': 'http://www.firexapp.com/',
        'flame_url': get_flame_url(cli_args.port),
    }


def main():
    signal.signal(signal.SIGTERM, _sigterm_handler)
    signal.signal(signal.SIGINT, _sigint_handler)

    args = _parse_args()
    _config_logging(args.logs_dir)
    t = threading.Timer(args.flame_timeout, _exit_on_timeout)
    try:
        t.start()
        logger.info('Starting Flame Server with args: %s' % args)
        run_flame(args.broker, args.port, _create_run_metadata(args), args.recording, args.broker_recv_ready_file,
                  args.broker_max_retry_attempts)
        t.cancel()
    except Exception as e:
        logger.exception(e)
    finally:
        logger.info("Shutting down Flame server.")
