"""
Flask API module for interacting with celery tasks.
"""

import logging

from flask import jsonify

from firex_flame.flame_helper import wait_until

from firex_flame.event_aggregator import slim_tasks_by_uuid, INCOMPLETE_STATES

from eventlet import spawn
from eventlet.green import subprocess, select

logger = logging.getLogger(__name__)

subprocess_dict = {}


def _run_metadata_to_api_model(run_metadata, root_uuid):
    return {
        'uid': run_metadata['uid'],
        'logs_dir': run_metadata['logs_dir'],
        'root_uuid': root_uuid,
        'chain': run_metadata['chain'],
        'logs_server': run_metadata['logs_server'],
    }


def _get_task_fields(tasks_by_uuid, fields):
    return {uuid: {f: v for f, v in task.items() if f in fields}
            for uuid, task in tasks_by_uuid.items()}


def poll_fd_readable(fd, timeout=0):
    readable, _, _ = select.select([fd], [], [], timeout)
    return readable


def monitor_file(sio_server, sid, host, filename):
    # To avoid issues with huge log files, we only get the last 50000 lines
    max_lines = 50000

    # spawn ssh to host to tail -f the file - output to be sent to requesting client
    logger.info("Will start monitoring file %s on host %s" % (filename, host))
    # noinspection PyBroadException
    try:
        # noinspection PyUnresolvedReferences
        with subprocess.Popen(["/bin/ssh", "-C", "-t", "-t", host,
                               """[ "$(/bin/find %s -perm -004)" ] && """
                               """/usr/bin/tail -n %d --follow=name %s 2>/dev/null || """
                               """echo "Access denied." """ % (filename, max_lines, filename)],
                              bufsize=0, stdout=subprocess.PIPE) as p:
            # Keep track of all spawned processes to be able to manage them later
            subprocess_dict[sid] = p

            # Wait up to 60s for data to start streaming in
            poll_fd_readable(p.stdout, 60)

            # Gather up the first bunch of log file data that comes in rapidly and it to the UI in one batch
            lines = []
            while poll_fd_readable(p.stdout, 5):
                try:
                    line = p.stdout.readline()
                    if not line or line == '':
                        break
                    lines.append(line.decode('utf-8', 'ignore'))
                except Exception:
                    logger.warning("Exception raised while trying to monitor file:", exc_info=True)
                    return
            num_lines = len(lines)
            if num_lines:
                if num_lines >= max_lines:
                    lines.insert(0, "[Showing only last %d lines]\n" % max_lines)
                else:
                    if num_lines != 1 or lines[0] == "Access denied.":
                        lines.insert(0, "[Beginning of file]\n")
                sio_server.emit('file-head', {'data': lines}, room=sid)

            # Now keep up with the 'live' incoming trickle of data
            while True:
                try:
                    line = p.stdout.readline()
                    if not line or line == '':
                        break
                    sio_server.emit('file-line', line.decode('utf-8', 'ignore'), room=sid)
                    num_lines += 1
                except Exception as e:
                    logger.warning("Exception raised while trying to monitor file:", exc_info=True)
                    return

            if num_lines:
                if num_lines != 1 or lines[0] == "Access denied.":
                    sio_server.emit('file-line', '[end of file - program exited]\n', room=sid)
            else:
                sio_server.emit('file-line', '[temporary file no longer exists because command has completed]\n',
                                room=sid)

    except Exception:
        sio_server.emit('file-line', "Spawned subprocess to monitor file failed.\n", room=sid)
        return


def term_subproc(sid):
    if sid not in subprocess_dict:
        logger.warning("SID %s not in subprocess list" % sid)
        return

    subproc = subprocess_dict[sid]
    subproc.terminate()
    del subprocess_dict[sid]


def term_all_subprocs():
    for sid in subprocess_dict:
        term_subproc(sid)


def create_socketio_task_api(sio_server, event_aggregator, run_metadata):

    @sio_server.on('send-graph-state')
    def emit_frontend_tasks_by_uuid(sid):
        """ Send 'slim' fields for all tasks. This allows visualization of the graph."""
        sio_server.emit('graph-state', slim_tasks_by_uuid(event_aggregator.tasks_by_uuid), room=sid)

    @sio_server.on('send-graph-fields')
    def emit_task_fields_by_uuid(sid, fields):
        """ Send the requested fields for all tasks."""
        response = _get_task_fields(event_aggregator.tasks_by_uuid, fields)
        sio_server.emit('graph-fields', response, room=sid)

    @sio_server.on('send-run-metadata')
    def emit_run_metadata(sid):
        """ Get static run-level data."""
        response = _run_metadata_to_api_model(run_metadata, event_aggregator.root_uuid)
        sio_server.emit('run-metadata', response, room=sid)

    @sio_server.on('send-task-details')
    def emit_detailed_tasks(sid, uuids):
        """ Get all fields for requested task UUIDs.
        Arguments:
            sid: The session ID to emit to.
            uuids (str or list of str): The uuid of the desired task to get details for.
        """
        if isinstance(uuids, str):
            uuid = uuids
            response = event_aggregator.tasks_by_uuid.get(uuid, None)
            sio_server.emit('task-details-' + uuid, response, room=sid)
        else:
            if not isinstance(uuids, list):
                response = []
            else:
                response = [event_aggregator.tasks_by_uuid.get(u, None) for u in uuids]
            sio_server.emit('task-details', response, room=sid)

    @sio_server.on('start-listen-file')
    def start_file_monitor(sid, args):
        if 'host' not in args or 'filepath' not in args:
            sio_server.emit('file-line',
                            "File monitoring request is missing either host and/or filepath parameters: %s" % args,
                            room=sid)
        else:
            spawn(monitor_file, sio_server=sio_server, sid=sid, host=args['host'], filename=args['filepath'])

    @sio_server.on('stop-listen-file')
    def stop_file_monitor(sid):
        term_subproc(sid)


def create_rest_task_api(web_app, event_aggregator, run_metadata):

    @web_app.route('/api/tasks')
    def get_all_tasks_by_uuid():
        return jsonify(slim_tasks_by_uuid(event_aggregator.tasks_by_uuid))

    # TODO: add /api/tasks?uuids=uuid1,uuid2, or POST with request body containing query by uuid
    @web_app.route('/api/tasks/<uuid>')
    def get_task_details(uuid):
        if uuid not in event_aggregator.tasks_by_uuid:
            return '', 404
        # if 'fields' in request['query']['fields']:
        #     return jsonify( _get_task_fields(event_aggregator.tasks_by_uuid, request['query']['fields']))
        # No fields were requested, so send all fields.
        return jsonify(event_aggregator.tasks_by_uuid[uuid])

    @web_app.route('/api/run-metadata')
    def get_run_metadata():
        return jsonify(_run_metadata_to_api_model(run_metadata, event_aggregator.root_uuid))


def create_revoke_api(sio_server, web_app, celery_app, tasks):

    @sio_server.on('revoke-task')
    def socketio_revoke_task(sid, uuid):
        logger.info("Received SocketIO request to revoke %s" % uuid)
        revoked = _revoke_task(uuid)
        response_event = 'revoke-success' if revoked else 'revoke-failed'
        sio_server.emit(response_event, room=sid)

    @web_app.route('/api/revoke/<uuid>')
    def rest_revoke_task(uuid):
        logger.info("Received REST request to revoke %s" % uuid)
        revoked = _revoke_task(uuid)
        return '', 200 if revoked else 500

    def _wait_until_task_complete(task, timeout, sleep_for=1):
        wait_until(lambda t: t['state'] not in INCOMPLETE_STATES, timeout, sleep_for, task)

    def _revoke_task(uuid):
        if uuid not in tasks:
            return False

        # Get the task instance
        task = tasks[uuid]

        # Try to revoke the task
        if task['state'] in INCOMPLETE_STATES:
            celery_app.control.revoke(uuid, terminate=True)
            logger.info("Submitted revoke to celery for: %s" % uuid)
        else:
            logger.info("Task %s already in terminal state %s" % (uuid, task['state']))

        # Wait for the task to become revoked
        revoke_timeout = 10
        _wait_until_task_complete(task, timeout=revoke_timeout)

        task_runstate = task['state']
        revoked = task_runstate == 'task-revoked'
        if not revoked:
            logger.warning("Failed to revoke task: waited %s sec and runstate is currently %s"
                           % (revoke_timeout, task_runstate))
        else:
            logger.debug("Successfully revoked task %s." % uuid)

        return revoked  # If the task was successfully revoked, return true
