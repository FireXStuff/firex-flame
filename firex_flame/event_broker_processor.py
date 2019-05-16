"""
Process events from Celery in to flame data model.
"""

import logging
import threading
import traceback

import json
import time
from celery.events import EventReceiver

logger = logging.getLogger(__name__)


class BrokerEventConsumerThread(threading.Thread):
    """Events threading class
    """
    def __init__(self, celery_app, flame_controller, event_aggregator, recording_file=None):
        threading.Thread.__init__(self)
        self.celery_app = celery_app
        self.recording_file = recording_file
        self.flame_controller = flame_controller
        self.event_aggregator = event_aggregator
        self.shutdown = False

    def run(self):
        self._run_from_broker()

    def _run_from_broker(self):
        """Load the events from celery"""
        # Loop to receive the events from celery.
        try_interval = 1
        while not self.shutdown:
            try:
                try_interval *= 2
                with self.celery_app.connection() as conn:
                    conn.ensure_connection(max_retries=1, interval_start=0)
                    recv = EventReceiver(conn,
                                         handlers={"*": self._on_celery_event},
                                         app=self.celery_app)
                    try_interval = 1
                    recv.capture(limit=None, timeout=None, wakeup=True)
            except (KeyboardInterrupt, SystemExit):
                try:
                    import _thread as thread
                except ImportError:
                    # noinspection PyUnresolvedReferences
                    import thread
                logger.info(traceback.format_exc())
                logger.info('Exiting main thread')
                thread.interrupt_main()
            except Exception:  # pylint: disable=broad-except
                if self.shutdown:
                    return
                logger.error(traceback.format_exc())
                time.sleep(try_interval)
            finally:
                if self.shutdown:
                    # Create new events that change the run state of incomplete events.
                    incomplete_task_events = self.event_aggregator.generate_incomplete_events()
                    self._aggregate_and_send(incomplete_task_events)
                    self.flame_controller.dump_data_model(self.event_aggregator.tasks_by_uuid)

    def _on_celery_event(self, event):
        """Callback function for when an event is received

        Arguments:
            event(dict): The event to aggregate and send downstream.
        """
        # Append the event to the recording file if it is specified
        if self.recording_file:
            with open(self.recording_file, "a") as rec:
                event_line = json.dumps(event)
                rec.write(event_line + "\n")

        if event.get('type', None) == "task-shutdown":
            self.shutdown = True
        self._aggregate_and_send([event])

    def _aggregate_and_send(self, events):
        new_data_by_task_uuid = self.event_aggregator.aggregate_events(events)
        self.flame_controller.send_event(new_data_by_task_uuid)
