import os
import subprocess
import urllib.parse

from firexapp.broker_manager.broker_factory import BrokerFactory
from firexapp.common import get_available_port, qualify_firex_bin, select_env_vars
from firexapp.submit.tracking_service import TrackingService
from firexapp.submit.console import setup_console_logging

from firex_flame.flame_helper import DEFAULT_FLAME_TIMEOUT, get_flame_debug_dir, \
    get_rec_file, get_flame_url, get_old_rec_file, web_request_ok
from firex_flame.model_dumper import is_dump_complete

logger = setup_console_logging(__name__)


def get_flame_args(port, uid, broker_recv_ready_file, args):
    if args.flame_record:
        rec_file = args.flame_record
    else:
        rec_file = get_rec_file(uid.logs_dir)

    # assemble startup cmd
    cmd_args = {
        'port': port,
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
        self.flame_url = None
        self.broker_recv_ready_file = None
        self.sync = None
        self.firex_logs_dir = None

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
                                default=None)
        arg_parser.add_argument('--flame_terminate_on_complete',
                                help='Terminate Flame when run completes. Ignores timeout arg entirely.',
                                default=None, const=True, nargs='?')

    def start(self, args, uid=None, **kwargs)->{}:
        port = int(args.flame_port) if args.flame_port else get_available_port()
        self.broker_recv_ready_file = os.path.join(get_flame_debug_dir(uid.logs_dir), 'celery_receiver_ready')
        self.sync = args.sync
        self.firex_logs_dir = uid.logs_dir

        flame_args = get_flame_args(port, uid, self.broker_recv_ready_file, args)

        # start the flame service and return the port
        try:
            subprocess.Popen([qualify_firex_bin("firex_flame")] + flame_args,
                             stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT,              
                             close_fds=True, env=select_env_vars(['PATH']))
        except Exception as e:
            logger.error("Flame subprocess start failed: %s." % e)
            raise

        self.flame_url = get_flame_url(port)

        if args.flame_central_server and args.flame_central_server_ui_path:
            display_url = url_join(args.flame_central_server, args.flame_central_server_ui_path, str(uid))
        else:
            display_url = self.flame_url

        logger.info('Flame: %s' % display_url)
        return {"flame_port": port}

    def ready_for_tasks(self, **kwargs) -> bool:
        webserver_alive = web_request_ok(urllib.parse.urljoin(self.flame_url, '/alive'))
        broker_recv_ready = os.path.isfile(self.broker_recv_ready_file)

        return webserver_alive and broker_recv_ready

    def ready_release_console(self, **kwargs) -> bool:
        if self.sync:
            # For sync requests, guarantee that the model is completely dumped before terminating.
            return is_dump_complete(self.firex_logs_dir)
        return True
