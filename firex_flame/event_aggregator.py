"""
Aggregates events in to the task data model.
"""

import logging

logger = logging.getLogger(__name__)


def _deep_merge_keys(dict1, dict2, keys):
    dict1_to_merge = {k: v for k, v in dict1.items() if k in keys}
    dict2_to_merge = {k: v for k, v in dict2.items() if k in keys}
    return _deep_merge(dict1_to_merge, dict2_to_merge)


def _both_instance(o1, o2, _type):
    return isinstance(o1, _type) and isinstance(o2, _type)


def _deep_merge(dict1, dict2):
    result = dict(dict1)
    for d2_key in dict2:
        if d2_key in dict1:
            v1 = dict1[d2_key]
            v2 = dict2[d2_key]
            if _both_instance(v1, v2, dict):
                result[d2_key] = _deep_merge(v1, v2)
            elif _both_instance(v1, v2, list):
                result[d2_key] = v1 + v2
            elif _both_instance(v1, v2, set):
                result[d2_key] = v1.union(v2)
            elif v1 == v2:
                # already the same value in both dicts, take from either.
                result[d2_key] = v1
            else:
                # Both d1 and d2 have entries for d2_key, both entries are not dicts or lists,
                # and the values are not the same. This is a conflict.
                # Overwrite d1's value to simulate dict.update() behaviour.
                result[d2_key] = v2
        else:
            # New key for d1, just add it.
            result[d2_key] = dict2[d2_key]
    return result


# Event data extraction/transformation without current state context.
def get_new_event_data(event):
    new_task_data = {}

    # TODO: change to a blacklist?
    copy_fields = ['hostname', 'parent_id', 'type', 'retries', 'firex_bound_args', 'flame_additional_data',
                   'actual_runtime', 'support_location', 'utcoffset', 'type',
                   'code_url',  # TODO: use code_filepath instead of code_url.
                   'firex_default_bound_args',  'from_plugin', 'chain_depth', 'firex_result', 'traceback',
                   'exception', 'long_name', 'flame_data']
    for field in copy_fields:
        if field in event:
            new_task_data[field] = event[field]

    state_types = ['task-received', 'task-blocked', 'task-started', 'task-succeeded', 'task-shutdown',
                   'task-failed', 'task-revoked', 'task-incomplete']
    fields_to_transforms = {
        'name': lambda e: {'name': e['name'].split('.')[-1], 'long_name': e['name']},
        'type': lambda e: {
            'state': e['type'],
            # TODO: run a task on a big run to see how much data this accumulates.
            # 'states': [{'state': e['type'], 'timestamp': e['timestamp']}],
        } if e['type'] in state_types else {},
        'url': lambda e: {'logs_url': e['url']}, # TODO: for backwards compat. only. Can use log_filepath.
        'log_filepath': lambda e: {'logs_url': e['log_filepath']},
        # Note first_started is never overwritten by downstream processing.
        'local_received': lambda e: {'first_started': e['local_received']}
    }
    for field, transform in fields_to_transforms.items():
        if field in event:
            new_task_data.update(transform(event))

    return {event['uuid']: new_task_data}


def find_data_changes(task, new_task_data):
    merge_keys = ['flame_data', 'states']
    no_overwrite_keys = ['first_started']
    # Some fields overwrite whatever is present.
    override_dict = {k: v for k, v in new_task_data.items() if k not in merge_keys + no_overwrite_keys}

    changed_data = {}
    for new_data_key, new_data_val in override_dict.items():
        if new_data_key not in task or task[new_data_key] != new_data_val:
            changed_data[new_data_key] = new_data_val

    # Some fields are dropped if there is already a value (no_overwrite).
    for no_overwrite_key in no_overwrite_keys:
        if no_overwrite_key in new_task_data and no_overwrite_key not in task:
            changed_data[no_overwrite_key] = new_task_data[no_overwrite_key]

    # Some fields need to be accumulated across events, not overwritten from latest event.
    merged_values = _deep_merge_keys(task, new_task_data, merge_keys)
    for merged_data_key, merged_data_val in merged_values.items():
        if merged_data_key not in task or task[merged_data_key] != merged_data_val:
            changed_data[merged_data_key] = merged_data_val

    return changed_data


def frontend_tasks_by_uuid(tasks_by_uuid):
    # These are the minimum fields required to render the graph.
    frontend_task_fields = ['uuid', 'state', 'children_uuids', 'task_num', 'name', 'flame_additional_data',
                            'first_started', 'hostname', 'actual_runtime', 'chain_depth', 'retries', 'from_plugin',
                            'parent_id', 'exception', 'flame_data']
    return {uuid: {k: v for k, v in task_data.items()
                   if k in frontend_task_fields}
            for uuid, task_data in tasks_by_uuid.items()}


class FlameEventAggregator:

    def __init__(self):
        self.tasks_by_uuid = {}
        self.new_task_num = 1

    def aggregate_events(self, events):
        new_data_by_task_uuid = {}
        for e in events:
            event_new_data_by_task_uuid = self._aggregate_event(e)
            for uuid, new_data in event_new_data_by_task_uuid.items():
                if uuid not in new_data_by_task_uuid:
                    new_data_by_task_uuid[uuid] = {}
                new_data_by_task_uuid[uuid].update(new_data)
        return new_data_by_task_uuid

    def generate_incomplete_events(self):
        incomplete_task_types = ['task-blocked', 'task-started', 'task-unblocked', 'task-received']
        return [{'uuid': t['uuid'], 'type': 'task-incomplete'}
                for t in self.tasks_by_uuid.values()
                if t['type'] in incomplete_task_types]

    def _get_or_create_task(self, task_uuid):
        if task_uuid not in self.tasks_by_uuid:
            task = {
                'uuid': task_uuid,
                'task_num': self.new_task_num,
            }
            self.new_task_num += 1
            self.tasks_by_uuid[task_uuid] = task
            is_new = True
        else:
            task = self.tasks_by_uuid[task_uuid]
            is_new = False
        return task, is_new

    def _aggregate_event(self, event):
        if ('uuid' not in event
                # revoked events can be sent before any other, and we'll never get any data (name, etc) for that task.
                or (event['uuid'] not in self.tasks_by_uuid and event.get('type', '') == 'task-revoked')):
            return {}

        new_data_by_task_uuid = get_new_event_data(event)
        result_by_uuid = {}
        for task_uuid, new_task_data in new_data_by_task_uuid.items():
            task, is_new_task = self._get_or_create_task(task_uuid)

            changed_data = find_data_changes(task, new_task_data)
            task.update(changed_data)

            # If we just created the task, we need to send the auto-initialized fields, as well as data from the event.
            # If this isn't a new event, we only need to send what has changed.
            result_by_uuid[task_uuid] = dict(task) if is_new_task else dict(changed_data)

        return result_by_uuid
