import os
import subprocess
import time

from firexapp.common import qualify_firex_bin, select_env_vars
from firexapp.submit.tracking_service import TrackingService
from firexapp.submit.console import setup_console_logging
from firexapp.submit.install_configs import FireXInstallConfigs

from firex_flame.flame_helper import DEFAULT_FLAME_TIMEOUT, get_flame_debug_dir, get_rec_file, is_json_file
from firex_flame.model_dumper import is_dump_complete, get_run_metadata_file, get_flame_url

logger = setup_console_logging(__name__)


def get_flame_args(uid, broker_recv_ready_file, args):
    if args.flame_record:
        rec_file = args.flame_record
    else:
        rec_file = get_rec_file(uid.logs_dir)

    # assemble startup cmd
    cmd_args = {
        'port': args.flame_port,
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
        'extra_task_dump_paths': args.flame_extra_task_dump_paths,
    }
    result = []
    for k, v in cmd_args.items():
        if v is not None:
            result.append('--%s' % k)
            result.append('%s' % v)
    return result


class FlameLauncher(TrackingService):

    def __init__(self):
        self.broker_recv_ready_file = None
        self.sync = None
        self.firex_logs_dir = None
        self.is_ready_for_tasks = False
        self.start_time = None
        self.stdout_file = None
        self.wait_for_webserver = None

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
        arg_parser.add_argument('--flame_wait_for_webserver',
                                help='Wait for webserver when waiting to be ready for tasks.',
                                default=None, const=True, nargs='?')
        arg_parser.add_argument('--flame_extra_task_dump_paths',
                                help='Paths specifying alternative task reprentations to dump at end of flame.',
                                default=None)

    def start(self, args, install_configs: FireXInstallConfigs, uid=None, **kwargs) -> dict:
        super().start(args, install_configs, uid=uid, **kwargs)

        flame_debug_dir = get_flame_debug_dir(uid.logs_dir)
        os.makedirs(flame_debug_dir, exist_ok=True)
        self.broker_recv_ready_file = os.path.join(flame_debug_dir, 'celery_receiver_ready')

        self.sync = args.sync
        self.firex_logs_dir = uid.logs_dir

        if args.flame_wait_for_webserver is None:
            self.wait_for_webserver = True
        else:
            self.wait_for_webserver = args.flame_wait_for_webserver

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
        if not self.is_ready_for_tasks:
            broker_ready = os.path.isfile(self.broker_recv_ready_file)

            if self.wait_for_webserver:
                run_metadata_file = get_run_metadata_file(firex_logs_dir=self.firex_logs_dir)
                webserver_ready = os.path.isfile(run_metadata_file) and is_json_file(run_metadata_file)
            else:
                webserver_ready = True

            self.is_ready_for_tasks = broker_ready and webserver_ready
            if self.is_ready_for_tasks:
                logger.debug("Flame up after %.2f s" % (time.time() - self.start_time))

                if self.wait_for_webserver:
                    # Only print Flame URL if we've waited for webserver, since otherwise we don't know the port.
                    logger.info(f"Flame: {self.get_viewer_url()}")

        return self.is_ready_for_tasks

    def ready_release_console(self, **kwargs) -> bool:
        if self.sync:
            # For sync requests, guarantee that the model is completely dumped before terminating.
            return is_dump_complete(self.firex_logs_dir)
        return True

    def get_viewer_url(self):
        if not self.install_configs.has_viewer():
            return get_flame_url(firex_logs_dir=self.firex_logs_dir)
        return self.install_configs.run_url
