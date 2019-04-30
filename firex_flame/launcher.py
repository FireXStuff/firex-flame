
from celery.utils.log import get_task_logger
from firexapp.submit.tracking_service import TrackingService

logger = get_task_logger(__name__)


class FlameLauncher(TrackingService):
    def __init__(self):
        self.sync = None

    def extra_cli_arguments(self, arg_parser):
        # todo: add the ArgParse arguments
        pass

    def start(self, args)->{}:
        logger.debug("Start flame")
        # todo: start the flame service and return the port
        self.sync = args.sync

    def __del__(self):
        if not self.sync:
            print('See Flame to monitor the status of your run at: %s')
