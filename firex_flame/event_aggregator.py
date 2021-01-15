"""
Aggregates events in to the task data model.
"""
from datetime import datetime
import logging

from firexapp.events.model import ADDITIONAL_CHILDREN_KEY, EXTERNAL_COMMANDS_KEY

from firex_flame.flame_helper import deep_merge

logger = logging.getLogger(__name__)

TASK_ARGS = 'firex_bound_args'

#
# config field options:
#   copy_celery - True if this field should be copied from the celery event to the task data model. If the field already
#                   has a value on the data model, more recent celery field values will overwrite existing values by
#                   default. If overwriting should be avoided, see 'aggregate_merge' and 'aggregate_keep_initial'
#                   options described below.
#
#   slim_field - True if this field should be included in the 'slim' (minimal) data model representation sent to the UI.
#                   Be very careful adding fields, since this field will be sent for each node and can therefore greatly
#                   increase the amount of data sent to the UI on main graph load.
#
#   transform_celery - A function to be executed on the entire event when the corresponding key is present in a celery
#                       event. The function returns a dict that dict.update the existing data from the event, possibly
#                       overwriting data copied from the celery event by copy_celery=True. Can be used to change
#                       the field name on the data model from the field name from celery.
#
#   aggregate_merge - True if model updates should deep merge collection data types (lists, dicts, sets) instead of
#                       overwriting.
#
#   aggregate_keep_initial - True if data model field updates should be ignored after an initial value has been set.
#                               This is one way of preventing overwriting, see also 'aggregate_merge'.
#
#
FIELD_CONFIG = {
    'uuid': {'copy_celery': True, 'slim_field': True},
    'hostname': {'copy_celery': True, 'slim_field': True},
    'parent_id': {'copy_celery': True, 'slim_field': True},
    'type': {
        'copy_celery': True,
        'transform_celery': lambda e: {
            'state': e['type'],
            'states': [{'state': e['type'], 'timestamp': e.get('local_received', None)}],
        } if e['type'] in STATE_TYPES else {},
    },
    'retries': {'copy_celery': True, 'slim_field': True},
    TASK_ARGS: {'copy_celery': True},
    'actual_runtime': {'copy_celery': True, 'slim_field': True},
    'support_location': {'copy_celery': True},
    'utcoffset': {'copy_celery': True},
    'code_url': {'copy_celery': True},
    # TODO: start using code_filepath instead of code_url.
    'code_filepath': {'copy_celery': True},
    'firex_default_bound_args': {'copy_celery': True},
    'from_plugin': {'copy_celery': True, 'slim_field': True},
    'chain_depth': {'copy_celery': True, 'slim_field': True},
    'firex_result': {'copy_celery': True},
    'traceback': {'copy_celery': True},
    'exception': {'copy_celery': True, 'slim_field': True},
    'long_name': {
        'copy_celery': True,
        'transform_celery': lambda e: {'name': e['long_name'].split('.')[-1]},
    },
    'flame_data': {
        'copy_celery': True,
        'slim_field': True,
        'aggregate_merge': True,
    },
    'state': {'slim_field': True},
    'task_num': {'slim_field': True},
    'name': {
        'slim_field': True,
        # TODO: firexapp should send long_name, since it will overwrite 'name' copied from celery. Then get rid of
        # the following config.
        'transform_celery': lambda e: {'name': e['name'].split('.')[-1], 'long_name': e['name']},
    },
    'called_as_orig': {
        'copy_celery': True,
    },
    'first_started': {'slim_field': True, 'aggregate_keep_initial': True},
    'url': {
        # TODO: only for backwards compat. Can use log_filepath.
        'transform_celery': lambda e: {'logs_url': e['url']},
    },
    'log_filepath': {
        'transform_celery': lambda e: {'logs_url': e['log_filepath']},
    },
    'local_received': {
        # Note first_started is never overwritten by aggregation.
        'transform_celery': lambda e: {'first_started': e['local_received']},
    },
    'states': {'aggregate_merge': True},
    'exception_cause_uuid': {'copy_celery': True},
    EXTERNAL_COMMANDS_KEY: {
        'copy_celery': True,
        'aggregate_merge': True,
    },
    ADDITIONAL_CHILDREN_KEY: {
        'copy_celery': True,
        'aggregate_merge': True,
        'slim_field': True,
    },
    'error_context': {
        'copy_celery': True,
        'slim_field': True,
    },
}


def _get_keys_with_true(input_dict, key):
    return [k for k, v in input_dict.items() if v.get(key, False)]


COPY_FIELDS = _get_keys_with_true(FIELD_CONFIG, 'copy_celery')
# These are the minimum fields required to render the graph.
SLIM_FIELDS = _get_keys_with_true(FIELD_CONFIG, 'slim_field')

AGGREGATE_MERGE_FIELDS = _get_keys_with_true(FIELD_CONFIG, 'aggregate_merge')
AGGREGATE_KEEP_INITIAL_FIELDS = _get_keys_with_true(FIELD_CONFIG, 'aggregate_keep_initial')
AGGREGATE_NO_OVERWRITE_FIELDS = AGGREGATE_MERGE_FIELDS + AGGREGATE_KEEP_INITIAL_FIELDS

FIELD_TO_CELERY_TRANSFORMS = {k: v['transform_celery'] for k, v in FIELD_CONFIG.items() if 'transform_celery' in v}

STATE_TYPES = {
    'task-received': {'terminal': False},
    'task-started': {'terminal': False},
    'task-blocked': {'terminal': False},
    'task-unblocked': {'terminal': False},
    'task-succeeded': {'terminal': True},
    'task-failed': {'terminal': True},
    'task-revoked': {'terminal': True},
    'task-incomplete': {'terminal': True},  # server-side kludge state to fix tasks that will never complete.
}
COMPLETE_STATES = [s for s, v in STATE_TYPES.items() if v['terminal']]
INCOMPLETE_STATES = [s for s, v in STATE_TYPES.items() if not v['terminal']]


def _deep_merge_keys(dict1, dict2, keys):
    dict1_to_merge = {k: v for k, v in dict1.items() if k in keys}
    dict2_to_merge = {k: v for k, v in dict2.items() if k in keys}
    return deep_merge(dict1_to_merge, dict2_to_merge)


# Event data extraction/transformation without current state context.
def get_new_event_data(event):
    new_task_data = {}
    for field in COPY_FIELDS:
        if field in event:
            new_task_data[field] = event[field]

    # Note if a field is both a copy field and a transform, the transform overrides if the output writes to the same
    # key.
    for field, transform in FIELD_TO_CELERY_TRANSFORMS.items():
        if field in event:
            new_task_data.update(transform(event))

    return {event['uuid']: new_task_data}


def find_data_changes(task, new_task_data):
    # Some fields overwrite whatever is present. Be permissive, since not all fields captured are from celery,
    # so not all have entries in the field config.
    override_dict = {k: v for k, v in new_task_data.items() if k not in AGGREGATE_NO_OVERWRITE_FIELDS}

    changed_data = {}
    for new_data_key, new_data_val in override_dict.items():
        if new_data_key not in task or task[new_data_key] != new_data_val:
            changed_data[new_data_key] = new_data_val

    # Some field updates are dropped if there is already a value for that field name (keep initial).
    for no_overwrite_key in AGGREGATE_KEEP_INITIAL_FIELDS:
        if no_overwrite_key in new_task_data and no_overwrite_key not in task:
            changed_data[no_overwrite_key] = new_task_data[no_overwrite_key]

    # Some fields need to be accumulated across events, not overwritten from latest event.
    merged_values = _deep_merge_keys(task, new_task_data, AGGREGATE_MERGE_FIELDS)
    for merged_data_key, merged_data_val in merged_values.items():
        if merged_data_key not in task or task[merged_data_key] != merged_data_val:
            changed_data[merged_data_key] = merged_data_val

    return changed_data


def slim_tasks_by_uuid(tasks_by_uuid):
    return {uuid: {k: v for k, v in task_data.items()
                   if k in SLIM_FIELDS}
            for uuid, task_data in tasks_by_uuid.items()}


class FlameEventAggregator:
    """ Aggregates many events in to the task data model. """

    def __init__(self):
        self.tasks_by_uuid = {}
        self.new_task_num = 1
        self.root_uuid = None

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
        """
        Unfortunately, if a run terminates ungracefully, incomplete tasks will never arrive at a
        terminal runstate. The 'task-incomplete' runstate is a fake (non-backend) terminal runstate
        that is generated here so that the UI can show a non-incomplete runstate.
        :return:
        """
        now = datetime.now().timestamp()
        return [{'uuid': task['uuid'],
                 'type': 'task-incomplete',
                 'actual_runtime': now - task.get('first_started', now)}
                for task in self.tasks_by_uuid.values()
                if task['state'] in INCOMPLETE_STATES]

    def is_root_complete(self):
        if not self.root_uuid or self.root_uuid not in self.tasks_by_uuid:
            return False  # Might not have root event yet.
        root_runstate = self.tasks_by_uuid[self.root_uuid].get('state', None)
        return root_runstate in COMPLETE_STATES

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
                # The uuid can be null, it's unclear what this means but it can't be associated with a task.
                or not event['uuid']
                # Revoked events can be sent before any other, and we'll never get any data (name, etc) for that task.
                # Therefore ignore events that are for a new UUID that have revoked type.
                or (event['uuid'] not in self.tasks_by_uuid and event.get('type', '') == 'task-revoked')):
            return {}

        if event.get('parent_id', '__no_match') is None and self.root_uuid is None:
            self.root_uuid = event['uuid']

        new_data_by_task_uuid = get_new_event_data(event)
        changes_by_task_uuid = {}
        for task_uuid, new_task_data in new_data_by_task_uuid.items():
            task, is_new_task = self._get_or_create_task(task_uuid)

            changed_data = find_data_changes(task, new_task_data)
            task.update(changed_data)

            # If we just created the task, we need to send the auto-initialized fields, as well as data from the event.
            # If this isn't a new event, we only need to send what has changed.
            changes_by_task_uuid[task_uuid] = dict(task) if is_new_task else dict(changed_data)

        return changes_by_task_uuid
