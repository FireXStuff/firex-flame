from glob import glob
import json
import logging
import os
import importlib_resources
import urllib.parse
from contextlib import ExitStack
import atexit

import socketio
from gevent import pywsgi
from geventwebsocket.handler import WebSocketHandler

from flask import Flask, redirect, send_from_directory, Response, render_template

from firex_flame.api import create_socketio_task_api, create_revoke_api, create_rest_task_api
from firexapp.submit.reporting import REL_COMPLETION_REPORT_PATH
from firex_flame.flame_helper import get_flame_url

logger = logging.getLogger(__name__)

# This static directory can be used during development to test unreleased versions of the UI.
static_files_dir = os.path.join(os.path.dirname(__file__), "static")

if os.path.isdir(static_files_dir):
    UI_RESOURCE_DIR = static_files_dir
else:
    # Load UI from pypi install case.
    file_manager = ExitStack()
    atexit.register(file_manager.close)
    UI_RESOURCE_DIR = str(file_manager.enter_context(importlib_resources.files('firex_flame_ui')))

REL_UI_RESOURCE_PATH = '/ui'

# Never changes per flame instance, populated on first request.
cached_ui_index_response = None


def get_ui_artifact_glob_basename(artifact_glob):
    return os.path.basename(glob(os.path.join(UI_RESOURCE_DIR, artifact_glob))[0])


def ui_resource_links_and_scripts(resource_root):
    app_css = get_ui_artifact_glob_basename('css/app.*.css')
    app_js = get_ui_artifact_glob_basename('js/app.*.js')
    chunk_vendors_js = get_ui_artifact_glob_basename('js/chunk-vendors.*.js')
    chunk_vendors_css = get_ui_artifact_glob_basename('css/chunk-vendors.*.css')
    head_links = [
        {'href': resource_root + '/css/' + app_css, 'rel': 'preload', 'as': 'style'},
        {'href': resource_root + '/js/' + app_js, 'rel': 'preload', 'as': 'script'},
        {'href': resource_root + '/js/' + chunk_vendors_js, 'rel': 'preload', 'as': 'script'},
        {'href': resource_root + '/css/' + chunk_vendors_css, 'rel': 'stylesheet', 'as': None},
        {'href': resource_root + '/css/' + app_css, 'rel': 'stylesheet', 'as': None},
    ]

    body_scripts = [
        {'src': resource_root + '/js/' + get_ui_artifact_glob_basename('js/chunk-vendors.*.js')},
        {'src': resource_root + '/js/' + get_ui_artifact_glob_basename('js/app.*.js')},
    ]
    return {
        'head_links': head_links,
        'body_scripts': body_scripts,
    }


class FlameResponse(Response):

    def __init__(self, response, **kwargs):
        # Avoid default octet-stream that causes download instead of display for some log files.
        if kwargs.get('mimetype', None) == 'application/octet-stream':
            kwargs['mimetype'] = 'text/plain'
        super(FlameResponse, self).__init__(response, **kwargs)


def create_ui_index_render_function(central_server, central_server_ui_path):
    if central_server and central_server_ui_path:
        central_server_ui_url = urllib.parse.urljoin(central_server, central_server_ui_path)
    else:
        central_server_ui_url = None

    def ui_root_render():
        global cached_ui_index_response

        if not cached_ui_index_response:
            relative_links_and_scripts = ui_resource_links_and_scripts(REL_UI_RESOURCE_PATH)

            if central_server_ui_url:
                central_links_and_scripts = ui_resource_links_and_scripts(central_server_ui_url)
            else:
                # When central valus not supplied, the index.html template produces an html file that references
                # only resources served by this server.
                central_links_and_scripts = {'head_links': None, 'body_scripts': None}

            cached_ui_index_response = render_template(
                'index.html',
                relative_head_links=relative_links_and_scripts['head_links'],
                relative_body_scripts=relative_links_and_scripts['body_scripts'],
                central_head_links=central_links_and_scripts['head_links'],
                central_body_scripts=central_links_and_scripts['body_scripts'])
        return cached_ui_index_response

    return ui_root_render


def ok_json_response(response_data):
    r = Response(response=json.dumps(response_data), status=200, mimetype="application/json")
    r.cache_control.max_age = 2592000
    return r


def create_ui_config(run_metadata):
    return {
        # Tells UI to fetch task data from the origin via Flame's socketio API.
        "access_mode": 'socketio-origin',
        "model_path_template": None,
        "is_central": False,
        "central_server": run_metadata['central_server'],
        "central_server_ui_path": run_metadata['central_server_ui_path'],
        "central_documentation_url": run_metadata['central_documentation_url'],
        "firex_bin": run_metadata['firex_bin'],
        "logs_serving": {
            "serve_mode": "central-webserver",
            "url_format": None,
        },
        'rel_completion_report_path': REL_COMPLETION_REPORT_PATH,
        "linkify_prefixes": ["/auto/", "/ws/"],
    }


def create_web_app(run_metadata, serve_logs_dir):
    web_app = Flask(__name__)
    web_app.response_class = FlameResponse
    web_app.secret_key = os.urandom(24)
    web_app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True

    # Redirect root path to UI.
    web_app.add_url_rule('/', 'flame_ui',
                         # TODO: set cache control.
                         create_ui_index_render_function(run_metadata['central_server'],
                                                         run_metadata['central_server_ui_path']))

    # Serve UI artifacts relatively.
    web_app.add_url_rule(REL_UI_RESOURCE_PATH + '/<path:f>', 'ui_artifacts',
                         lambda f: send_from_directory(UI_RESOURCE_DIR, f))

    # img assets are un-prefixed in build artifacts, so redirect to served img directory.
    web_app.add_url_rule('/img/<path:p>', 'ui_img_artifacts', lambda p: redirect(REL_UI_RESOURCE_PATH + '/img/' + p))

    ui_config = create_ui_config(run_metadata)
    web_app.add_url_rule('/flame-ui-config.json', 'ui_config', lambda: ok_json_response(ui_config))

    # Trivial 'OK' endpoint for testing if the server is up.
    web_app.add_url_rule('/alive', 'alive', lambda: ('', 200))

    # Redirect for old URLs. These links should be updated in emails etc, then this redirect removed.
    web_app.add_url_rule('/root/<uuid>', 'subtree', lambda uuid: redirect('/#/root/' + uuid))

    if serve_logs_dir:
        # Add directory listings and file serve for logs.
        from flask import Blueprint
        from flask_autoindex import AutoIndexBlueprint
        auto_index = Blueprint('auto_index', __name__)
        AutoIndexBlueprint(auto_index, browse_root=run_metadata['logs_dir'])
        web_app.register_blueprint(auto_index, url_prefix=run_metadata['logs_dir'])

    return web_app


def start_web_server(webapp_port, event_aggregator, run_metadata, controller, celery_app, serve_logs_dir: bool):
    web_app = create_web_app(run_metadata, serve_logs_dir)
    # TODO: parametrize cors_allowed_origins.
    sio_server = socketio.Server(cors_allowed_origins='*', async_mode='gevent')
    sio_web_app = socketio.WSGIApp(sio_server, web_app)
    controller.sio_server = sio_server

    create_socketio_task_api(controller, event_aggregator, run_metadata)
    create_rest_task_api(web_app, event_aggregator, run_metadata)

    server = pywsgi.WSGIServer(('', webapp_port), sio_web_app, handler_class=WebSocketHandler)
    server.start()  # Need to start() to get port if supplied 0.

    if celery_app:
        # Celery app means this is initial launch, not a replay from a rec file.
        run_metadata['flame_url'] = get_flame_url(server.server_port)
        # Can only dump initial metadata now that flame_url is set.
        controller.dump_initial_metadata()
        create_revoke_api(sio_server, web_app, celery_app, event_aggregator.tasks_by_uuid)

    return server
