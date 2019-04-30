import subprocess
from celery.utils.log import get_task_logger
from firex_flame import FLAME_LOG_REGISTRY_KEY
from firexapp.fileregistry import FileRegistry
from firexapp.submit.tracking_service import TrackingService

logger = get_task_logger(__name__)


class FlameLauncher(TrackingService):
    def __init__(self):
        self.sync = None

    def extra_cli_arguments(self, arg_parser):
        # todo: add the ArgParse arguments
        pass

    def start(self, args, uid=None, **kwargs)->{}:
        # store sync state for later
        self.sync = args.sync

        # assemble startup cmd
        cmd = "firex_flame"

        # start the flame service and return the port
        logger.debug("Starting Flame...")
        flame_stdout = FileRegistry().get_file(FLAME_LOG_REGISTRY_KEY, uid.logs_dir)
        with open(flame_stdout, 'wb') as out:
            subprocess.check_call(cmd, shell=True, stdout=out, stderr=subprocess.STDOUT)

    def __del__(self):
        if not self.sync:
            print('See Flame to monitor the status of your run at: %s')
