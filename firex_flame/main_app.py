import logging
import os
import sys
from threading import Thread

import celery
import socketio
import eventlet

from firex_flame.api import create_socketio_task_api, create_revoke_api, create_rest_task_api
from firex_flame.controller import FlameAppController
from firex_flame.event_file_processor import process_recording_file
from firex_flame.event_broker_processor import BrokerEventConsumerThread
from firex_flame.event_aggregator import FlameEventAggregator
from firex_flame.web_app import create_web_app

logger = logging.getLogger(__name__)


def run_flame(broker, web_port, run_metadata, recording_file, broker_receiver_ready_file, broker_max_retry_attempts):
    web_app = create_web_app(run_metadata['logs_dir'],
                             run_metadata['central_server'],
                             run_metadata['central_server_ui_path'])
    sio_server = socketio.Server()
    sio_web_app = socketio.Middleware(sio_server, web_app)

    event_aggregator = FlameEventAggregator()
    if recording_file and os.path.isfile(recording_file):
        event_recv_thread = Thread(target=process_recording_file, args=(event_aggregator, recording_file))
    else:
        assert broker, "Since recording file doesn't exist, the broker is required."
        celery_app = celery.Celery(broker=broker)
        controller = FlameAppController(sio_server, run_metadata)
        event_recv_thread = BrokerEventConsumerThread(celery_app,
                                                      controller,
                                                      event_aggregator,
                                                      recording_file,
                                                      broker_receiver_ready_file,
                                                      broker_max_retry_attempts,
                                                      )
        create_revoke_api(sio_server, web_app, celery_app, event_aggregator.tasks_by_uuid)

    create_socketio_task_api(sio_server, event_aggregator, run_metadata)
    create_rest_task_api(web_app, event_aggregator, run_metadata)
    event_recv_thread.start()

    try:
        eventlet.wsgi.server(eventlet.listen(('', web_port)), sio_web_app)
    except KeyboardInterrupt:
        logger.info('KeyboardInterrupt - Shutting down')
        sys.exit(0)
