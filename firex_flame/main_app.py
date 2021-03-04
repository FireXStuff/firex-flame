import logging
import os
from threading import Thread

import celery
from firexapp.broker_manager.broker_factory import RedisManager

from firex_flame.controller import FlameAppController
from firex_flame.event_file_processor import process_recording_file
from firex_flame.event_broker_processor import BrokerEventConsumerThread
from firex_flame.event_aggregator import FlameEventAggregator
from firex_flame.flame_helper import wait_until_path_exist

logger = logging.getLogger(__name__)


def create_broker_consumer_thread(broker_consumer_config, controller, event_aggregator,
                                  recording_file, shutdown_handler, logs_dir):
    try:
        broker_url = RedisManager.get_broker_url_from_logs_dir(logs_dir)
    except:
        logger.error(f"Failed to load broker URL from logs dir: {logs_dir}")
        raise
    else:
        celery_app = celery.Celery(broker=broker_url)
        return BrokerEventConsumerThread(celery_app,
                                         controller,
                                         event_aggregator,
                                         broker_consumer_config,
                                         recording_file,
                                         shutdown_handler)


def start_flame(webapp_port, broker_consumer_config, run_metadata, recording_file, shutdown_handler,
                extra_task_dump_paths, serve_logs_dir):
    event_aggregator = FlameEventAggregator()
    controller = FlameAppController(run_metadata, extra_task_dump_paths)
    if not recording_file or not os.path.isfile(recording_file):
        event_recv_thread = create_broker_consumer_thread(broker_consumer_config, controller, event_aggregator,
                                                          recording_file, shutdown_handler, run_metadata['logs_dir'])
        celery_app = event_recv_thread.celery_app
    else:
        event_recv_thread = Thread(target=process_recording_file, args=(event_aggregator, recording_file, run_metadata))
        celery_app = None
    event_recv_thread.start()

    if broker_consumer_config.receiver_ready_file:
        wait_until_path_exist(broker_consumer_config.receiver_ready_file, sleep_for=0.1)

    # Delaying of importing of all web dependencies is a deliberate startup performance optimization.
    # The broker should be listening for events as quickly as possible.
    from firex_flame.web_app import start_web_server
    return start_web_server(webapp_port, event_aggregator, run_metadata, controller, celery_app, serve_logs_dir)
