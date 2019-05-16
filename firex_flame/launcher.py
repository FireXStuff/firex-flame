import os
from socket import gethostname
import subprocess


from celery.utils.log import get_task_logger
from firex_flame import FLAME_LOG_REGISTRY_KEY
from firexapp.broker_manager.broker_factory import BrokerFactory
from firexapp.common import get_available_port
from firexapp.fileregistry import FileRegistry
from firexapp.submit.tracking_service import TrackingService
from firexapp.submit.console import setup_console_logging

logger = setup_console_logging(__name__)


def get_flame_url(port, hostname=gethostname()):
    return 'http://%s:%d' % (hostname, int(port))


class FlameLauncher(TrackingService):
    def __init__(self):
        self.sync = None
        self.port = -1

    def extra_cli_arguments(self, arg_parser):
        # todo: add the ArgParse arguments
        pass

    def start(self, args, port=None, uid=None, **kwargs)->{}:
        # store sync & port state for later
        self.sync = args.sync
        self.port = int(port) if port else get_available_port()
        rec_file = os.path.join(uid.logs_dir, 'flame2.rec')

        # assemble startup cmd
        cmd_args = {
            'port': self.port,
            'broker': BrokerFactory.get_broker_url(),
            'uid': str(uid),
            'logs_dir': uid.logs_dir,
            'recording': rec_file,
            'central_server': kwargs.get('central_firex_server', None),
        }

        non_empty_args_strs = ['--%s %s' % (k, v) for k, v in cmd_args.items() if v]
        cmd = 'firex_flame %s &' % ' '.join(non_empty_args_strs)

        # start the flame service and return the port
        logger.debug("Flame command: %s" % cmd)
        flame_stdout = FileRegistry().get_file(FLAME_LOG_REGISTRY_KEY, uid.logs_dir)
        with open(flame_stdout, 'wb') as out:
            subprocess.check_call(cmd, shell=True, stdout=out, stderr=subprocess.STDOUT)
        logger.info('Flame: %s' % get_flame_url(self.port))

    # TODO: this mechanism is unreliable.
    def __del__(self):
        if not self.sync:
            print('See Flame to monitor the status of your run at: %s' % get_flame_url(self.port))
