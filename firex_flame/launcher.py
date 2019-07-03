import os
import subprocess
import time
import urllib.parse

from firexapp.broker_manager.broker_factory import BrokerFactory
from firexapp.common import get_available_port
from firexapp.fileregistry import FileRegistry
from firexapp.submit.tracking_service import TrackingService
from firexapp.submit.console import setup_console_logging
from firexapp.submit.uid import Uid

from firex_flame.flame_helper import DEFAULT_FLAME_TIMEOUT, wait_until_web_request_ok, get_flame_debug_dir, \
    wait_until_path_exist, get_rec_file, get_flame_url

FLAME_LOG_REGISTRY_KEY = 'FLAME_OUTPUT_LOG_REGISTRY_KEY2'
FileRegistry().register_file(FLAME_LOG_REGISTRY_KEY, os.path.join(Uid.debug_dirname, 'flame2.stdout'))

logger = setup_console_logging(__name__)


def wait_webserver_and_celery_recv_ready(flame_url, broker_recv_ready_file):
    start_wait = time.perf_counter()

    # Wait for web server via HTTP GET.
    webserver_wait_timeout = 10
    webserver_alive = wait_until_web_request_ok(urllib.parse.urljoin(flame_url, '/alive'),
                                                timeout=webserver_wait_timeout, sleep_for=0.5)
    if not webserver_alive:
        raise Exception("Flame web server at %s not up after %s seconds." % (flame_url, webserver_wait_timeout))

    # Wait for broker ready file to be created by flame.
    celery_recvr_timeout = 5
    broker_recv_ready = wait_until_path_exist(broker_recv_ready_file, timeout=celery_recvr_timeout, sleep_for=0.5)
    if not broker_recv_ready:
        raise Exception("Flame celery receiver not ready after %s seconds." % celery_recvr_timeout)

    end_wait = time.perf_counter()
    logger.debug("Waited %.1f seconds for web server & celery receiver to become ready." % (end_wait - start_wait))


class FlameLauncher(TrackingService):
    def __init__(self):
        self.sync = None
        self.port = -1

    def extra_cli_arguments(self, arg_parser):
        arg_parser.add_argument('--flame_timeout', help='How long the webserver should run for, in seconds.',
                                default=DEFAULT_FLAME_TIMEOUT)
        arg_parser.add_argument('--flame_central_server',
                                help='Server URL from which flame resources can be fetched to enable browser caching'
                                     'and client-side settings.',
                                default=None)
        arg_parser.add_argument('--flame_central_server_ui_path',
                                help='Path relative to flame_central_server from which the Flame UI is served.',
                                default=None)
        arg_parser.add_argument('--broker_max_retry_attempts',
                                help='See Flame argument help.',
                                default=None)
        arg_parser.add_argument('--flame_record',
                                help='A file to record flame events',
                                default=None)
        arg_parser.add_argument('--flame_port',
                                help='Flame port to be used', type=int,
                                default=None)

    def start(self, args, uid=None, **kwargs)->{}:
        # store sync & port state for later
        self.sync = args.sync
        self.port = int(args.flame_port) if args.flame_port else get_available_port()
        if args.flame_record:
            rec_file = args.flame_record
        else:
            rec_file = get_rec_file(uid.logs_dir)
        broker_recv_ready_file = os.path.join(get_flame_debug_dir(uid.logs_dir), 'celery_receiver_ready')

        # assemble startup cmd
        cmd_args = {
            'port': self.port,
            'broker': BrokerFactory.get_broker_url(),
            'uid': str(uid),
            'logs_dir': uid.logs_dir,
            'chain': args.chain,
            'recording': rec_file,
            'central_server': args.flame_central_server,
            'central_server_ui_path': args.flame_central_server_ui_path,
            'flame_timeout': args.flame_timeout,
            'broker_recv_ready_file': broker_recv_ready_file,
            'broker_max_retry_attempts': args.broker_max_retry_attempts,
        }

        non_empty_args_strs = ['--%s %s' % (k, v) for k, v in cmd_args.items() if v]
        cmd = 'firex_flame %s &' % ' '.join(non_empty_args_strs)

        # start the flame service and return the port
        flame_stdout = FileRegistry().get_file(FLAME_LOG_REGISTRY_KEY, uid.logs_dir)
        with open(flame_stdout, 'wb') as out:
            subprocess.check_call(cmd, shell=True, stdout=out, stderr=subprocess.STDOUT)

        flame_url = get_flame_url(self.port)
        wait_webserver_and_celery_recv_ready(flame_url, broker_recv_ready_file)
        logger.info('Flame: %s' % flame_url)
        return {"flame_port": self.port}
