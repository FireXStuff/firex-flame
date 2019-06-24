from glob import glob
import logging
import os
import pkg_resources
import urllib.parse

from flask import Flask, Blueprint, redirect, send_from_directory, Response, render_template
from flask_autoindex import AutoIndexBlueprint

logger = logging.getLogger(__name__)

UI_RESOURCE_DIR = pkg_resources.resource_filename('firex_flame_ui', './')
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
                # central_resource_root = 'http://firex.cisco.com/auto/firex/flame'
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


def create_web_app(logs_dir, central_server, central_server_ui_path):
    web_app = Flask(__name__)
    web_app.response_class = FlameResponse
    web_app.secret_key = os.urandom(24)
    web_app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True

    # Redirect root path to UI.
    web_app.add_url_rule('/', 'flame_ui',
                         create_ui_index_render_function(central_server, central_server_ui_path))

    # Serve UI artifacts relatively.
    web_app.add_url_rule(REL_UI_RESOURCE_PATH + '/<path:f>', 'ui_artifacts',
                         lambda f: send_from_directory(UI_RESOURCE_DIR, f))

    # img assets are un-prefixed in build artifacts, so redirect to served img directory.
    web_app.add_url_rule('/img/<path:p>', 'ui_img_artifacts', lambda p: redirect(REL_UI_RESOURCE_PATH + '/img/' + p))

    # Trivial 'OK' endpoint for testing if the server is up.
    web_app.add_url_rule('/alive', 'alive', lambda: ('', 200))

    # Redirect for old URLs. These links should be updated in emails etc, then this redirect removed.
    web_app.add_url_rule('/root/<uuid>', 'subtree', lambda uuid: redirect('/#/root/' + uuid))

    # Add directory listings and file serve for logs.
    auto_index = Blueprint('auto_index', __name__)
    AutoIndexBlueprint(auto_index, browse_root=logs_dir)
    web_app.register_blueprint(auto_index, url_prefix=logs_dir)

    return web_app
