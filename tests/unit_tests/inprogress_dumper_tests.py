import os
import unittest
import tempfile

from firex_flame.controller import FlameAppController
from firex_flame.event_broker_processor import RunningModelDumper
from firex_flame.model_dumper import load_slim_tasks, get_full_task_path, get_tasks_slim_file, load_full_task

test_data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")


class TaskQueryTests(unittest.TestCase):

    def test_write_only_slim(self):
        with tempfile.TemporaryDirectory() as log_dir:
            flame_controller = FlameAppController({'logs_dir': log_dir}, [])
            uuid = '1'
            all_tasks_by_uuid = {uuid: {'uuid': uuid, 'name': 'hello'}}

            running_model_dumper = RunningModelDumper(flame_controller, all_tasks_by_uuid)
            running_model_dumper.queue_write_slim()
            running_model_dumper._queue.join()

            loaded_slim_tasks = load_slim_tasks(log_dir)
            self.assertEqual(all_tasks_by_uuid, loaded_slim_tasks)
            self.assertFalse(os.path.exists(get_full_task_path(log_dir, uuid)))

    def test_write_only_tasks(self):
        with tempfile.TemporaryDirectory() as log_dir:
            flame_controller = FlameAppController({'logs_dir': log_dir}, [])
            uuid1 = '1'
            uuid2 = '2'
            uuid3 = '3'
            all_tasks_by_uuid = {
                uuid1: {'uuid': uuid1, 'name': 'hello', 'flame_data': 'some_data'},
                uuid2: {'uuid': uuid2, 'name': 'hello again', 'flame_data': 'some other data'},
                uuid3: {'uuid': uuid3, 'name': 'another name', 'flame_data': 'some other data'},
            }

            running_model_dumper = RunningModelDumper(flame_controller, all_tasks_by_uuid)
            running_model_dumper.queue_maybe_write_tasks({uuid1: 'task-started-info',
                                                          uuid2: 'task-completed',
                                                          uuid3: 'task-blocked', # this dumper will not write this task.
                                                          })
            running_model_dumper._queue.join()

            self.assertFalse(os.path.exists(get_tasks_slim_file(log_dir)))

            self.assertEqual(all_tasks_by_uuid[uuid1], load_full_task(log_dir, uuid1))
            self.assertEqual(all_tasks_by_uuid[uuid2], load_full_task(log_dir, uuid2))

            # uuid3 not dumped due to event_type='task-blocked'
            self.assertFalse(os.path.exists(get_full_task_path(log_dir, uuid3)))

            # Expect uuid to be remaining, written on write_remaining_and_wait_stop.
            running_model_dumper.write_remaining_and_wait_stop()
            self.assertEqual(all_tasks_by_uuid[uuid3], load_full_task(log_dir, uuid3))

    def test_always_write_full_task_after_completed(self):
        with tempfile.TemporaryDirectory() as log_dir:
            flame_controller = FlameAppController({'logs_dir': log_dir}, [])
            uuid1 = '1'
            initial_task = {'uuid': uuid1, 'name': 'hello', 'flame_data': 'some_data'}
            all_tasks_by_uuid = {
                uuid1: initial_task,
            }

            running_model_dumper = RunningModelDumper(flame_controller, all_tasks_by_uuid)
            running_model_dumper.queue_maybe_write_tasks({uuid1: 'task-started-info'})

            running_model_dumper._queue.join()
            self.assertEqual(initial_task, load_full_task(log_dir, uuid1))

            first_updated_task = {'more_data': 1, **initial_task}
            all_tasks_by_uuid[uuid1] = first_updated_task
            running_model_dumper.queue_maybe_write_tasks({uuid1: 'task-blocked'})
            running_model_dumper._queue.join()
            # Expect data unchanged due to task-blocked.
            self.assertEqual(initial_task, load_full_task(log_dir, uuid1))

            running_model_dumper.queue_maybe_write_tasks({uuid1: 'task-completed'})
            running_model_dumper._queue.join()
            self.assertEqual(first_updated_task, load_full_task(log_dir, uuid1))

            second_updated_task = {'other_more_data': 2, **first_updated_task}
            all_tasks_by_uuid[uuid1] = second_updated_task
            running_model_dumper.queue_maybe_write_tasks({uuid1: 'task-blocked'})
            running_model_dumper._queue.join()
            # After a task-completed, task-blocked will cause updates.
            self.assertEqual(second_updated_task, load_full_task(log_dir, uuid1))