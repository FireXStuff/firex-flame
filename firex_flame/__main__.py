import argparse
import logging
import signal
import sys
from threading import Timer

from firex_flame.main_app import run_flame

# TODO: configure logging.
logger = logging.getLogger(__name__)


def _parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', help='Port for starting the web server on', type=int, default=8000)
    parser.add_argument('--uid', help='Unique identifier for the represented FireX run.')
    parser.add_argument('--logs_dir', help='Logs directory.', default=None)
    parser.add_argument('--recording', help='A file containing the recording of celery events.', default=None)
    parser.add_argument('--central_server', help='A central web server from which the UI and logs can be served.',
                        default=None)
    # TODO: could validate either rec file exists or broker is supplied.
    parser.add_argument('--broker', help='Celery broker.', default=None)
    parser.add_argument('--timeout', help='Maximum lifetime of this service, in seconds', type=int,
                        default=60 * 60 * 24 * 2)
    return parser.parse_args()


def _sigterm_handler(_, __):
    logger.info('SIGTERM detected, shutting down')
    sys.exit(0)


def _exit_on_timeout():
    print("Exiting on timeout")
    sys.exit(0)


def main():
    args = _parse_args()
    # TODO: add 'chain' property here?
    run_metadata = {'uid': args.uid, 'logs_dir': args.logs_dir, 'central_server': args.central_server}
    signal.signal(signal.SIGTERM, _sigterm_handler)
    t = Timer(args.timeout, _exit_on_timeout)
    try:
        t.start()
        print('Starting Flame Server.')
        run_flame(args.broker, args.port, run_metadata, args.recording)
        t.cancel()
    finally:
        print("Shutting down Flame server.")
