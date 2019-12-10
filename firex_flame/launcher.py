import os
import subprocess
import urllib.parse
import time

from firexapp.broker_manager.broker_factory import BrokerFactory
from firexapp.common import qualify_firex_bin, select_env_vars
from firexapp.submit.tracking_service import TrackingService
from firexapp.submit.console import setup_console_logging

from firex_flame.flame_helper import DEFAULT_FLAME_TIMEOUT, get_flame_debug_dir, \
    get_rec_file, web_request_ok
from firex_flame.model_dumper import is_dump_complete, get_flame_url

logger = setup_console_logging(__name__)


def get_flame_args(uid, broker_recv_ready_file, args):
    if args.flame_record:
        rec_file = args.flame_record
    else:
        rec_file = get_rec_file(uid.logs_dir)

    # assemble startup cmd
    cmd_args = {
        'port': args.flame_port,
        'broker': BrokerFactory.get_broker_url(),
        'uid': str(uid),
        'logs_dir': uid.logs_dir,
        'chain': args.chain,
        'recording': rec_file,
        'central_server': args.flame_central_server,
        'central_server_ui_path': args.flame_central_server_ui_path,
        'logs_server': args.flame_logs_server,
        'central_documentation_url': args.flame_central_documentation_url,
        'flame_timeout': args.flame_timeout,
        'broker_recv_ready_file': broker_recv_ready_file,
        'broker_max_retry_attempts': args.broker_max_retry_attempts,
        'terminate_on_complete': args.flame_terminate_on_complete,
        'firex_bin_path': args.firex_bin_path,
    }
    result = []
    for k, v in cmd_args.items():
        if v is not None:
            result.append('--%s' % k)
            result.append('%s' % v)
    return result


def url_join(host, start_path, end_path):
    if start_path.endswith('/'):
        path = start_path + end_path
    else:
        path = '%s/%s' % (start_path, end_path)
    return urllib.parse.urljoin(host, path)


class FlameLauncher(TrackingService):

    def __init__(self):
        self.broker_recv_ready_file = None
        self.sync = None
        self.firex_logs_dir = None
        self.is_ready_for_tasks = False
        self.start_time = None
        self.stdout_file = None

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
        arg_parser.add_argument('--flame_logs_server',
                                help='Server URL from which flame logs can be fetched.',
                                default=None)
        arg_parser.add_argument('--flame_central_documentation_url',
                                help='URL linking to main out-of-app docs.',
                                default=None)
        arg_parser.add_argument('--firex_bin_path',
                                help='Path to firex executable.',
                                default=None)
        arg_parser.add_argument('--broker_max_retry_attempts',
                                help='See Flame argument help.',
                                default=None)
        arg_parser.add_argument('--flame_record',
                                help='A file to record flame events',
                                default=None)
        arg_parser.add_argument('--flame_port',
                                help='Flame port to be used', type=int,
                                default=0)
        arg_parser.add_argument('--flame_terminate_on_complete',
                                help='Terminate Flame when run completes. Ignores timeout arg entirely.',
                                default=None, const=True, nargs='?')

    def start(self, args, uid=None, **kwargs) -> dict:
        flame_debug_dir = get_flame_debug_dir(uid.logs_dir)
        os.makedirs(flame_debug_dir, exist_ok=True)
        self.broker_recv_ready_file = os.path.join(flame_debug_dir, 'celery_receiver_ready')

        self.sync = args.sync
        self.firex_logs_dir = uid.logs_dir

        flame_args = get_flame_args(uid, self.broker_recv_ready_file, args)
        self.stdout_file = os.path.join(flame_debug_dir, 'flame.stdout')

        self.start_time = time.time()
        try:
            with open(self.stdout_file, 'w+') as f:
                subprocess.Popen([qualify_firex_bin("firex_flame")] + flame_args,
                                 stdout=f, stderr=subprocess.STDOUT,
                                 close_fds=True, env=select_env_vars(['PATH']))
        except Exception as e:
            logger.error("Flame subprocess start failed: %s." % e)
            raise

        return {}

    def ready_for_tasks(self, **kwargs) -> bool:
        if self.is_ready_for_tasks:
            return True

        flame_url = get_flame_url(firex_logs_dir=self.firex_logs_dir)
        if flame_url is not None:
            self.is_ready_for_tasks = os.path.isfile(self.broker_recv_ready_file)

            if self.is_ready_for_tasks:
                # This will only be printed once due to initial guard.
                logger.info('Flame: %s' % flame_url)
                logger.warning("Flame up after %.2f s" % (time.time() - self.start_time))

        return self.is_ready_for_tasks

    def ready_release_console(self, **kwargs) -> bool:
        if self.sync:
            # For sync requests, guarantee that the model is completely dumped before terminating.
            return is_dump_complete(self.firex_logs_dir)
        return True
