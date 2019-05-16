import logging
import os

from flask import Flask, Blueprint, redirect, send_from_directory, Response
from flask.ext.autoindex import AutoIndexBlueprint


logger = logging.getLogger(__name__)


class FlameResponse(Response):

    def __init__(self, response, **kwargs):
        # Avoid default octet-stream that causes download instead of display for some log files.
        if kwargs.get('mimetype', None) == 'application/octet-stream':
            kwargs['mimetype'] = 'text/plain'
        super(FlameResponse, self).__init__(response, **kwargs)


def create_web_app(logs_dir):
    web_app = Flask(__name__)
    # TODO: externalize ui root/get artifacts from dependency.
    ui_root_dir = 'ui'
    web_app.response_class = FlameResponse
    web_app.secret_key = os.urandom(24)

    # Redirect root path to UI.
    web_app.add_url_rule('/', 'flame_ui', lambda: redirect("/ui/index.html", code=302))

    # Serve UI artifacts.
    web_app.add_url_rule('/ui/<path:f>', 'ui_root', lambda f: send_from_directory(ui_root_dir, f))

    # Add directory listings and file serve for logs.
    auto_index = Blueprint('auto_index', __name__)
    AutoIndexBlueprint(auto_index, browse_root=logs_dir)
    web_app.register_blueprint(auto_index, url_prefix=logs_dir)

    return web_app
