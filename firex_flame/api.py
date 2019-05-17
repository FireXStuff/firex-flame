"""
Flask API module for interacting with celery tasks.
"""

import time
import logging

from flask import jsonify

from firex_flame.event_aggregator import frontend_tasks_by_uuid, INCOMPLETE_STATES

logger = logging.getLogger(__name__)


def create_socketio_task_api(sio_server, tasks, run_metadata):

    @sio_server.on('send-graph-state')
    def emit_frontend_tasks_by_uuid(sid):
        """ Send the full state."""
        sio_server.emit('graph-state', frontend_tasks_by_uuid(tasks), room=sid)

    @sio_server.on('send-run-metadata')
    def emit_run_metadata(sid):
        """ Get static run-level data."""
        # TODO: root is now captured in event_aggregator, refer to it here.
        root_task = next(filter(lambda n: n.get('parent_id', '__dont_match_default__') is None,
                                tasks.values()),
                         {})
        if 'firex_bound_args' in root_task and 'chain' in root_task['firex_bound_args']:
            chain = root_task['firex_bound_args']['chain']
        else:
            chain = None
        response = {
            'uid': run_metadata['uid'],
            'logs_dir': run_metadata['logs_dir'],
            'root_uuid': root_task.get('uuid', None),
            'chain': chain,
            'centralServer': run_metadata['central_server'],
        }
        sio_server.emit('run-metadata', response, room=sid)

    @sio_server.on('send-task-details')
    def emit_detailed_tasks(sid, uuids):
        """Get the desired task structure

        Arguments:
            sid: The session ID to use.
            uuid(str): The uuid of the desired task to get details for.
        """
        if isinstance(uuids, str):
            uuid = uuids
            response = tasks.get(uuid, None)
            sio_server.emit('task-details-' + uuid, response, room=sid)
        else:
            if not isinstance(uuids, list):
                response = []
            else:
                response = [tasks.get(u, None) for u in uuids]
            sio_server.emit('task-details', response, room=sid)


def create_rest_task_api(web_app, tasks):

    @web_app.route('/api/tasks')
    def all_tasks_by_uuid():
        return jsonify(frontend_tasks_by_uuid(tasks))


def create_revoke_api(sio_server, celery_app, tasks):

    @sio_server.on('revoke-task')
    def socket_revoke_task(sid, uuid):
        response = _revoke_task(uuid)
        socket_event = 'revoke-success' if response else 'revoke-failed'
        sio_server.emit(socket_event, room=sid)

    def _revoke_task(uuid):
        if uuid not in tasks:
            return False

        # Get the task instance
        task = tasks[uuid]

        # Try to revoke the task
        if task['state'] in INCOMPLETE_STATES:
            celery_app.control.revoke(uuid, terminate=True)

        # Wait for the task to become revoked
        check_timeout = 5
        check = 1
        while task['state'] in INCOMPLETE_STATES:
            time.sleep(1)
            if check > check_timeout:
                break
            else:
                check += 1

        # If the task was successfully revoked, return true
        return task['state'] == 'task-revoked'
