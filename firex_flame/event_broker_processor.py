"""
Process events from Celery in to flame data model.
"""

import logging
from pathlib import Path
import threading
import traceback
import json
import time

from celery.events import EventReceiver

from firex_flame.controller import FlameAppController
from firex_flame.flame_helper import BrokerConsumerConfig

logger = logging.getLogger(__name__)


class BrokerEventConsumerThread(threading.Thread):
    """Events threading class
    """
    def __init__(self, celery_app, flame_controller: FlameAppController, event_aggregator, config: BrokerConsumerConfig,
                 recording_file: str, shutdown_handler):
        threading.Thread.__init__(self, daemon=True)
        self.celery_app = celery_app
        self.recording_file = recording_file
        self.flame_controller = flame_controller
        self.event_aggregator = event_aggregator
        self.max_try_interval = 2**config.max_retry_attempts if config.max_retry_attempts is not None else 32
        self.terminate_on_complete = config.terminate_on_complete
        self.stopped_externally = False
        self.shutdown_handler = shutdown_handler

        if config.receiver_ready_file:
            self.receiver_ready_file = Path(config.receiver_ready_file)
            assert not self.receiver_ready_file.exists(), \
                "Receiver ready file must not already exist: %s." % self.receiver_ready_file
        else:
            self.receiver_ready_file = None
        self.is_first_receive = True

    def _create_receiver_ready_file(self):
        if self.is_first_receive:
            if self.receiver_ready_file:
                self.receiver_ready_file.touch()
            self.is_first_receive = False

    def _cleanup_tasks(self):
        # Create new events that change the run state of incomplete events.
        incomplete_task_events = self.event_aggregator.generate_incomplete_events()
        if incomplete_task_events:
            logger.warning("Forcing runstates of %d incomplete tasks to be terminal." % len(incomplete_task_events))
            self._aggregate_and_send(incomplete_task_events)
        else:
            logger.debug("All tasks already terminal following terminal root.")

    def run(self):
        self._run_from_broker()

    def _cleanup(self):
        try:
            self._cleanup_tasks()
            self.flame_controller.dump_complete_data_model(self.event_aggregator)
        except Exception as e:
            logger.error("Failed to cleanup during receiver completion.")
            logger.exception(e)
        finally:
            logger.info("Completed receiver cleanup.")
            if self.terminate_on_complete and not self.stopped_externally:
                self.shutdown_handler.shutdown("Terminating on completion, as requested by input args.")

    def _run_from_broker(self):
        """Load the events from celery"""
        try:
            self._capture_events()
        finally:
            self._cleanup()

    def _capture_events(self):
        try_interval = 1
        while not self.event_aggregator.is_root_complete():
            try:
                try_interval *= 2
                with self.celery_app.connection() as conn:
                    conn.ensure_connection(max_retries=1, interval_start=0)
                    recv = EventReceiver(conn,
                                         handlers={"*": self._on_celery_event},
                                         app=self.celery_app)
                    try_interval = 1
                    self._create_receiver_ready_file()
                    recv.capture(limit=None, timeout=None, wakeup=True)
            except (KeyboardInterrupt, SystemExit) as e:
                self.stopped_externally = True
                self.shutdown_handler.shutdown(str(e))
            # pylint: disable=C0321
            except Exception:
                if self.event_aggregator.is_root_complete():
                    logger.info("Root task complete; stopping broker receiver (not entire server).")
                    return
                logger.error(traceback.format_exc())
                if try_interval > self.max_try_interval:
                    logger.warning("Maximum broker retry attempts exceeded, stopping receiver (not entire server)."
                                   " Will no longer retry despite incomplete root task.")
                    return
                logger.debug("Try interval %d secs, still worth retrying." % try_interval)
                time.sleep(try_interval)

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
        self._aggregate_and_send([event])

    def _aggregate_and_send(self, events):
        new_data_by_task_uuid = self.event_aggregator.aggregate_events(events)
        self.flame_controller.send_sio_event(new_data_by_task_uuid, self.event_aggregator.tasks_by_uuid)
