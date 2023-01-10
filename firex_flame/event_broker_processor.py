"""
Process events from Celery in to flame data model.
"""

import logging
from pathlib import Path
import threading
import traceback
import json
import time
from typing import Optional
from dataclasses import dataclass
from enum import Enum
from typing import Any

from celery.events import EventReceiver
from gevent import spawn, sleep, spawn_later
from gevent.queue import JoinableQueue

from firex_flame.controller import FlameAppController
from firex_flame.flame_helper import BrokerConsumerConfig

logger = logging.getLogger(__name__)

EXTRA_TASK_REPR_DUMP_DELAY_STEP = 5

class QueueItemType(Enum):
    SLIM_DUMP_TYPE = 'SLIM'
    TASK_DUMP_TYPE = 'TASK'
    STOP_DUMP_TYPE = 'STOP'
    EXTRA_REPR_DUMP_TYPE = 'EXTRA_REPR_DUMP_TYPE'


@dataclass
class _QueueItem:
    item_type : QueueItemType
    task_uuid: Optional[str] = None
    celery_event_type: Optional[str] = None


class RunningModelDumper:
    """
    Maintains a queue and a greenlet for async writing the flame data model.
    Having a dedicated greenlet prevents writing the model to disk
    from blocking the main celery event receiver.
    """

    WRITE_EVENT_TYPES = ['task-started', 'task-started-info', 'task-completed']

    def __init__(
        self,
        flame_controller: FlameAppController,
        all_tasks_by_uuid: dict[str, dict[str, Any]],
        max_extra_task_repr_dump_delay: int = 5*60,
    ):
        self.flame_controller = flame_controller

        # This is a JoinableQueue just to make testing easier. Clients will wait on the greenlet that processes
        # queue items, not the queue itself.
        self._queue : JoinableQueue[_QueueItem] = JoinableQueue()
        self.all_tasks_by_uuid = all_tasks_by_uuid

        # This class uses the 'task-completed' event type to loosely infer task completeness, since there is no
        # stronger indicator (e.g. task-failed does not mean completed, due to retries). This field stores task UUIDs
        # of tasks that have at one moment in time seen a task-completed event, which is distinct from tasks whose
        # latest event type is 'task-completed', since 'task-succeeded' can occur after 'task-completed'. Every event
        # after 'task-completed' will cause an additional write, since we don't know what can come after. This
        # is less than ideal.
        self.seen_task_completed_uuids : set[str] = set()
        self._consume_queue_greenlet = spawn(self._consume_from_queue)

        # Ideally task representations would only be dumped when they change,
        # but to do that efficiently the FlameTaskGraph should be incrementally
        # updated as Celery events come in instead of calculated from scratch.
        #
        # For now, just dump extra reprs periodically. See Github Issue #8.
        if flame_controller.extra_task_representations:
            self._schedule_dump_extra_task_representation(0, max_extra_task_repr_dump_delay)

    def _dump_full_task(self, uuid: str, task: dict[str, Any]) -> None:
        try:
            self.flame_controller.dump_full_task(uuid, task)
        except OSError as e:
            logger.warning(f"Failed to write {uuid} full task JSON: {e}")

    def _schedule_dump_extra_task_representation(self, prev_delay: int, max_delay: int):
        delay_sec = min(prev_delay + EXTRA_TASK_REPR_DUMP_DELAY_STEP, max_delay) # linear backoff
        spawn_later(delay_sec, self._dump_extra_task_representation, delay_sec, max_delay)

    def _dump_extra_task_representation(self, delay_sec: int, max_delay: int):
        self.flame_controller.dump_extra_task_representations(self.all_tasks_by_uuid)
        # Schedule the next dump
        self._schedule_dump_extra_task_representation(delay_sec, max_delay)

    def _maybe_dump_task(self, uuid: str, event_type: str):
        if uuid in self.all_tasks_by_uuid:
            task = self.all_tasks_by_uuid[uuid]
            if (
                event_type in self.WRITE_EVENT_TYPES
                or uuid in self.seen_task_completed_uuids
            ):
                self._dump_full_task(uuid, task)
                if event_type == 'task-completed':
                    self.seen_task_completed_uuids.add(uuid)
        else:
            logger.warning(f"Failed to write non-existant task with uuid: {uuid}")

    def _deduplicate_and_maybe_write_full_tasks(self, task_dump_work_items: list[_QueueItem]) -> None:
        deduplicated_task_uuid_to_event_type : dict[str, str] = {}
        for task_work_item in task_dump_work_items:
            uuid = task_work_item.task_uuid
            event_type = task_work_item.celery_event_type
            assert uuid is not None, "Must have task UUID for TASK_DUMP_TYPE"
            assert event_type is not None, "Must have event type for TASK_DUMP_TYPE"

            cur_uuid_event_type = deduplicated_task_uuid_to_event_type.get(uuid)
            if (
                not cur_uuid_event_type
                # Never change away from a type that will cause a write
                or cur_uuid_event_type not in self.WRITE_EVENT_TYPES
                # always write completed events since we track this to not
                # miss events that arrive after completed.
                or event_type == 'task-completed'
            ):
                deduplicated_task_uuid_to_event_type[uuid] = event_type

        for uuid, event_type in deduplicated_task_uuid_to_event_type.items():
            self._maybe_dump_task(uuid, event_type)

    def _get_all_from_queue(self) -> list[_QueueItem]:
        item = self._queue.get()

        # drain queue and process all work items at once, de-duplicating work.
        return [item] + [self._queue.get() for _ in range(len(self._queue))]

    def _consume_from_queue(self) -> None:
        while True:
            # drain queue and process all work items at once, de-duplicating work.
            work_items : list[_QueueItem] = self._get_all_from_queue()
            work_item_types : list[QueueItemType] = {t.item_type for t in work_items}

            try:
                if QueueItemType.SLIM_DUMP_TYPE in work_item_types:
                    self.flame_controller.dump_slim_tasks(self.all_tasks_by_uuid)

                if QueueItemType.TASK_DUMP_TYPE in work_item_types:
                    self._deduplicate_and_maybe_write_full_tasks(
                        [t for t in work_items if t.item_type == QueueItemType.TASK_DUMP_TYPE],
                    )

                if QueueItemType.STOP_DUMP_TYPE in work_item_types:
                    all_uuids = set(self.all_tasks_by_uuid.keys())
                    uuids_without_completed = all_uuids.difference(self.seen_task_completed_uuids)
                    for uuid in uuids_without_completed:
                        self._dump_full_task(uuid, self.all_tasks_by_uuid[uuid])

            except Exception as e:
                # TODO: narrow exception handling so that an error in handling of one dump_type doesn't fail others.
                logger.error("Failure while processing task-dumping work queue entry.")
                logger.exception(e)
            finally:
                for _ in range(len(work_items)):
                    self._queue.task_done()

                # Must be last, want to process all other work items before we stop processing all future
                # work items.
                if QueueItemType.STOP_DUMP_TYPE in work_item_types:
                    logger.debug(f"Stopping in progress model dumper.")
                    break

                # Let other greenlets run, possibly let work accumulate in the queue to allow work de-duplication
                sleep(0.2)

    def queue_write_slim(self) -> None:
        self._queue.put(_QueueItem(QueueItemType.SLIM_DUMP_TYPE))

    def queue_maybe_write_tasks(self, task_uuids_to_event_types):
        for task_uuid, event_type in task_uuids_to_event_types.items():
            self._queue.put(_QueueItem(QueueItemType.TASK_DUMP_TYPE, task_uuid, event_type))

    def write_remaining_and_wait_stop(self) -> None:
        self.queue_write_slim()
        self._queue.put(_QueueItem(QueueItemType.STOP_DUMP_TYPE))
        self._consume_queue_greenlet.join() # Wait for queue to drain.


class BrokerEventConsumerThread(threading.Thread):
    """Events threading class
    """
    def __init__(
        self,
        celery_app,
        flame_controller: FlameAppController,
        event_aggregator,
        config: BrokerConsumerConfig,
        recording_file: str,
        shutdown_handler,
    ):
        threading.Thread.__init__(self, daemon=True)
        self.celery_app = celery_app
        self.recording_file = recording_file
        self.flame_controller = flame_controller
        self.event_aggregator = event_aggregator
        self.max_try_interval = 2**config.max_retry_attempts if config.max_retry_attempts is not None else 32
        self.terminate_on_complete = config.terminate_on_complete
        self.stopped_externally = False
        self.shutdown_handler = shutdown_handler
        self._event_count = 0

        self.receiver_ready_file : Optional[Path]

        if config.receiver_ready_file:
            self.receiver_ready_file = Path(config.receiver_ready_file)
            assert not self.receiver_ready_file.exists(), \
                f"Receiver ready file must not already exist: {self.receiver_ready_file}"
        else:
            self.receiver_ready_file = None
        self.is_first_receive = True
        self.running_dumper_queue = RunningModelDumper(self.flame_controller, self.event_aggregator.tasks_by_uuid)

    def _create_receiver_ready_file(self):
        if self.receiver_ready_file:
            self.receiver_ready_file.touch()

    def _cleanup_tasks(self):
        # Create new events that change the run state of incomplete events.
        incomplete_task_events = self.event_aggregator.generate_incomplete_events()
        if incomplete_task_events:
            logger.warning("Forcing runstates of %d incomplete tasks to be terminal." % len(incomplete_task_events))
            self._aggregate_and_send(incomplete_task_events)
        else:
            logger.debug("All tasks already terminal following terminal root.")

    def run(self):
        """Listen for events from celery"""
        try:
            self._capture_events()
        finally:
            self._cleanup()

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
                logger.debug(f"Try interval {try_interval} secs, still worth retrying.")
                time.sleep(try_interval)

    def _on_celery_event(self, event):
        """Callback function for when an event is received

        Arguments:
            event(dict): The event to aggregate and send downstream.
        """
        if self.is_first_receive:
            # Hopefully this is 'flame-indicate-ready' event
            self._create_receiver_ready_file()
            self.is_first_receive = False

        # Append the event to the recording file if it is specified
        if self.recording_file:
            with open(self.recording_file, "a") as rec:
                event_line = json.dumps(event)
                rec.write(event_line + "\n")

        if self._event_count % 100 == 0:
            logger.debug(f'Received Celery event number {self._event_count}'
                         f' with task uuid: {event.get("uuid")}')
        self._event_count += 1

        self._aggregate_and_send([event])

    def _aggregate_and_send(self, events):
        new_data_by_task_uuid = self.event_aggregator.aggregate_events(events)
        slim_update_data_by_uuid = self.flame_controller.send_sio_event(new_data_by_task_uuid,
                                                                        self.event_aggregator.tasks_by_uuid)
        if slim_update_data_by_uuid:
            self.running_dumper_queue.queue_write_slim()

        self.running_dumper_queue.queue_maybe_write_tasks(
            {u: self.event_aggregator.tasks_by_uuid.get(u, {}).get('type')
             for u in new_data_by_task_uuid.keys()})
