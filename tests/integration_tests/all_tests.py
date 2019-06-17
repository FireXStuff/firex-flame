import abc
import re
import os
import psutil
import signal
import time
import requests
import urllib.parse

import socketio
from firexapp.testing.config_base import FlowTestConfiguration, assert_is_good_run
from firexapp.submit.submit import get_log_dir_from_output

from firex_flame.event_file_processor import get_tasks_from_log_dir
from firex_flame.flame_helper import get_flame_pid, wait_until_pid_not_exist, wait_until, get_rec_file

def flame_url_from_output(cmd_output):
    m = re.search(r'Flame: (http://.*)\n', cmd_output)
    if m:
        return m.group(1)
    return None


def kill_flame(log_dir, sig=signal.SIGKILL):
    flame_pid = get_flame_pid(log_dir)
    if psutil.pid_exists(flame_pid):
        os.kill(flame_pid, sig)
        wait_until_pid_not_exist(flame_pid, timeout=10)
    return flame_pid


class FlameFlowTestConfiguration(FlowTestConfiguration):
    __metaclass__ = abc.ABCMeta

    def assert_expected_firex_output(self, cmd_output, cmd_err):
        log_dir = get_log_dir_from_output(cmd_output)
        flame_url = flame_url_from_output(cmd_output)
        assert flame_url, "Found no Flame URL in cmd_output"
        try:
            self.assert_on_flame_url(log_dir, flame_url)
        finally:
            # do teardown here until there is a real teardown method.
            kill_flame(log_dir)

    @abc.abstractmethod
    def assert_on_flame_url(self, log_dir, flame_url):
        pass

    def assert_expected_return_code(self, ret_value):
        assert_is_good_run(ret_value)


class FlameLaunchesTest(FlameFlowTestConfiguration):
    """Verifies Flame is launched with nop chain by requesting a couple web endpoints."""

    def __init__(self):
        self.chain = 'nop'
        super().__init__()

    def initial_firex_options(self) -> list:
        return ["submit", "--chain", self.chain]

    def assert_on_flame_url(self, log_dir, flame_url):
        assert get_flame_pid(log_dir) > 0, "Found no pid: %s" % get_flame_pid(log_dir)

        alive_url = urllib.parse.urljoin(flame_url, '/alive')
        alive_request = requests.get(alive_url)
        assert alive_request.ok, 'Expected OK response when fetching alive resource page: %s' % alive_url

        root_request = requests.get(flame_url)
        assert root_request.ok, 'Expected OK response when fetching main page: %s' % root_request.status_code
        # TODO: could query for all linked resources to make sure their accessible, but that might be overkill.

        tasks_api_request = requests.get(flame_url + '/api/tasks')
        assert tasks_api_request.status_code == 200
        task_names = [t['name'] for t in tasks_api_request.json().values()]
        assert self.chain in task_names, "Task API endpoint didn't include executed chain (%s)." % self.chain


class FlameTimeoutShutdownTest(FlameFlowTestConfiguration):
    """ Flame shuts itself down when a given timeout is exceeded."""

    def __init__(self):
        self.flame_timeout = 2
        super().__init__()

    def initial_firex_options(self) -> list:
        return ["submit", "--chain", 'nop', '--flame_timeout', str(self.flame_timeout)]

    def assert_on_flame_url(self, log_dir, flame_url):
        time.sleep(self.flame_timeout + 1)
        flame_pid = get_flame_pid(log_dir)
        assert not psutil.pid_exists(flame_pid), "Timeout should have killed flame pid, but it still exists."


class FlameSigtermShutdownTest(FlameFlowTestConfiguration):
    """ Flame shutsdown cleanly when getting a sigterm."""

    def initial_firex_options(self) -> list:
        return ["submit", "--chain", 'nop']

    def assert_on_flame_url(self, log_dir, flame_url):
        flame_pid = get_flame_pid(log_dir)
        assert psutil.pid_exists(flame_pid), "Flame pid should exist before being killed by SIGTERM."
        kill_flame(log_dir, sig=signal.SIGTERM)
        assert not psutil.pid_exists(flame_pid), "SIGTERM should have caused flame to terminate, but pid still exists."


class FlameSigintShutdownTest(FlameFlowTestConfiguration):
    """ Flame shutsdown cleanly when getting a sigint."""

    def initial_firex_options(self) -> list:
        return ["submit", "--chain", 'nop']

    def assert_on_flame_url(self, log_dir, flame_url):
        flame_pid = get_flame_pid(log_dir)
        assert psutil.pid_exists(flame_pid), "Flame pid should exist before being killed by SIGINT."
        kill_flame(log_dir, sig=signal.SIGINT)
        assert not psutil.pid_exists(flame_pid), "SIGINT should have caused flame to terminate, but pid still exists."


def wait_until_root_exists(log_dir, timeout=20, sleep_for=1):
    def does_root_exist(log_dir):
        if not os.path.isfile(get_rec_file(log_dir)):
            return False
        _, root_uuid = get_tasks_from_log_dir(log_dir)
        return root_uuid is not None

    return wait_until(does_root_exist, timeout, sleep_for, log_dir)


class FlameRevokeTest(FlameFlowTestConfiguration):
    """ Uses Flame SocketIO API to revoke a run. """

    # Don't run with --sync, since this test will revoke the incomplete root task.
    sync = False

    def initial_firex_options(self) -> list:
        return ["submit", "--chain", 'sleep', '--sleep', '60']

    def assert_on_flame_url(self, log_dir, flame_url):
        root_exists = wait_until_root_exists(log_dir)
        assert root_exists, "Root task doesn't exist in the flame rec file, something is wrong with run."
        tasks, root_uuid = get_tasks_from_log_dir(log_dir)
        root_task_runstate = tasks[root_uuid]['state']
        assert root_task_runstate == 'task-incomplete', "Expected incomplete root, but found %s" % root_task_runstate

        sio_client = socketio.Client()
        resp = {'success': None}

        SUCCESS_EVENT = 'revoke-success'

        @sio_client.on(SUCCESS_EVENT)
        def revoke_success(_):
            resp['success'] = SUCCESS_EVENT

        @sio_client.on('revoke-failed')
        def revoke_failed(_):
            resp['success'] = 'revoke-failed'

        sio_client.connect(flame_url)
        sio_client.emit('revoke-task', data=root_uuid)
        wait_until(lambda: resp['success'] is not None, timeout=60, sleep_for=1)
        sio_client.disconnect()

        assert resp['success'] == SUCCESS_EVENT, "Expected response %s but received %s" \
                                                 % (SUCCESS_EVENT, resp['success'])

        # Need to re-parse task data, since now it should be revoked
        tasks_by_uuid, root_uuid = get_tasks_from_log_dir(log_dir)
        root_runstate = tasks_by_uuid[root_uuid]['state']
        assert root_runstate == 'task-revoked', "Expected root runstate to be revoked, was %s" % root_runstate
