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
from gevent import spawn, sleep
from gevent.queue import JoinableQueue

from firex_flame.controller import FlameAppController
from firex_flame.flame_helper import BrokerConsumerConfig

logger = logging.getLogger(__name__)


class RunningModelDumper:
    """
    Maintains a queue and a greenlet for async writing the flame data model. This prevents writing the model to disk
    from blocking the main celery event receiver.
    """

    SLIM_DUMP_TYPE = 'SLIM'
    TASK_DUMP_TYPE = 'TASK'
    STOP_DUMP_TYPE = 'STOP'
    ALL_WITHOUT_COMPLETED_DUMP_TYPE = 'ALL_WITHOUT_COMPLETED'
    ALL_DUMP_TYPES = {SLIM_DUMP_TYPE, TASK_DUMP_TYPE, STOP_DUMP_TYPE, ALL_WITHOUT_COMPLETED_DUMP_TYPE}

    def __init__(self,  flame_controller: FlameAppController, all_tasks_by_uuid):
        self.flame_controller = flame_controller

        # This is a JoinableQueue just to make testing easier. Clients will wait on the greenlet that processes
        # queue items, not the queue itself.
        self._queue = JoinableQueue()
        self.all_tasks_by_uuid = all_tasks_by_uuid

        # This class uses the 'task-completed' event type to loosely infer task completeness, since there is no
        # stronger indicator (e.g. task-failed does not mean completed, due to retries). This field stores task UUIDs
        # of tasks that have at one moment in time seen a task-completed event, which is distinct from tasks whose
        # latest event type is 'task-completed', since 'task-succeeded' can occur after 'task-completed'. Every event
        # after 'task-completed' will cause an additional write, since we don't know what can come after. This
        # is less than ideal.
        self.seen_task_completed_uuids = set()
        self._greenlet = spawn(self._consume_from_queue)

    def _dump_full_task(self, uuid, task):
        try:
            self.flame_controller.dump_full_task(uuid, task)
        except OSError as e:
            logger.warning(f"Failed to write {uuid} full task JSON: {e}")

    def _maybe_dump_task(self, uuid, event_type):
        if uuid in self.all_tasks_by_uuid:
            task = self.all_tasks_by_uuid[uuid]
            if (event_type in ['task-started-info', 'task-completed']
                    or uuid in self.seen_task_completed_uuids):
                self._dump_full_task(uuid, task)
                if event_type == 'task-completed':
                    self.seen_task_completed_uuids.add(uuid)
        else:
            logger.warning(f"Failed to write non-existant task with uuid: {uuid}")

    def _deduplicate_and_maybe_write_full_tasks(self, task_dump_work_items):
        deduplicated_task_uuid_to_event_type = {}
        for task_work_item in task_dump_work_items:
            uuid = task_work_item[1]
            event_type = task_work_item[2]
            assert uuid is not None, "Must have task UUID for TASK_DUMP_TYPE"
            assert event_type is not None, "Must have event type for TASK_DUMP_TYPE"

            # Never change away from task-completed, since we need to track if we've ever seen this type,
            # per task, to reduce total full task writes.
            if deduplicated_task_uuid_to_event_type.get(uuid) != 'task-completed':
                deduplicated_task_uuid_to_event_type[uuid] = event_type

        for uuid, event_type in deduplicated_task_uuid_to_event_type.items():
            self._maybe_dump_task(uuid, event_type)

    def _consume_from_queue(self):
        while True:
            triplet = self._queue.get()

            # drain queue and process all work items at once, de-duplicating work.
            work_items = [triplet] + [self._queue.get() for _ in range(len(self._queue))]
            work_item_types = {t[0] for t in work_items}

            try:
                if self.SLIM_DUMP_TYPE in work_item_types:
                    self.flame_controller.dump_slim_tasks(self.all_tasks_by_uuid)

                if self.TASK_DUMP_TYPE in work_item_types:
                    self._deduplicate_and_maybe_write_full_tasks([t for t in work_items if t[0] == self.TASK_DUMP_TYPE])

                if self.ALL_WITHOUT_COMPLETED_DUMP_TYPE in work_item_types:
                    all_uuids = set(self.all_tasks_by_uuid.keys())
                    uuids_without_completed = all_uuids.difference(self.seen_task_completed_uuids)
                    for uuid in uuids_without_completed:
                        self._dump_full_task(uuid, self.all_tasks_by_uuid[uuid])

                invalid_dump_types = work_item_types.difference(self.ALL_DUMP_TYPES)
                for invalid_dump_type in invalid_dump_types:
                    logger.warning(f"Unknown in-progress dumper work queue entry with type {invalid_dump_type}")

            except Exception as e:
                # TODO: narrow exception handling so that an error in handling of one dump_type doesn't fail others.
                logger.error("Failure while processing task-dumping work queue entry.")
                logger.exception(e)
            finally:
                for _ in range(len(work_items)):
                    self._queue.task_done()

                # Must be last.
                if self.STOP_DUMP_TYPE in work_item_types:
                    logger.debug(f"Stopping in progress model dumper.")
                    break

                # Let other greenlets run, possibly let work accumulate in the queue to allow work de-duplication
                sleep(0.2)

    def queue_write_slim(self):
        self._queue.put((self.SLIM_DUMP_TYPE, None, None))

    def queue_maybe_write_tasks(self, task_uuids_to_event_types):
        for task_uuid, event_type in task_uuids_to_event_types.items():
            self._queue.put((self.TASK_DUMP_TYPE, task_uuid, event_type))

    def write_remaining_and_wait_stop(self):
        self.queue_write_slim()
        self._queue.put((self.ALL_WITHOUT_COMPLETED_DUMP_TYPE, None, None))
        self._queue.put((self.STOP_DUMP_TYPE, None, None))
        self._greenlet.join() # Wait for queue to drain.

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
        self.running_dumper_queue = RunningModelDumper(self.flame_controller, self.event_aggregator.tasks_by_uuid)

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
            self.running_dumper_queue.write_remaining_and_wait_stop()
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
                if self.shutdown_handler.shutdown_received:
                    self.stopped_externally = True
                    logger.info("Shutdown handler received shutdown request; stopping broker receiver.")
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
        slim_update_data_by_uuid = self.flame_controller.send_sio_event(new_data_by_task_uuid,
                                                                        self.event_aggregator.tasks_by_uuid)
        if slim_update_data_by_uuid:
            self.running_dumper_queue.queue_write_slim()
        self.running_dumper_queue.queue_maybe_write_tasks({u: self.event_aggregator.tasks_by_uuid.get(u, {}).get('type')
                                                           for u in new_data_by_task_uuid.keys()})
