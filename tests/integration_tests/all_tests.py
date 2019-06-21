import abc
import re
import os
import psutil
import signal
import time
import requests
import urllib.parse

from bs4 import BeautifulSoup
import socketio
from firexapp.testing.config_base import FlowTestConfiguration, assert_is_good_run, skip_test
from firexapp.submit.submit import get_log_dir_from_output

from firex_flame.event_file_processor import get_tasks_from_log_dir
from firex_flame.flame_helper import get_flame_pid, wait_until_pid_not_exist, wait_until, get_rec_file
from firex_flame.event_aggregator import INCOMPLETE_STATES


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


def assert_all_match_some_prefix(strs, allowed_prefixes):
    for string in strs:
        if not any(string.startswith(ignore) for ignore in allowed_prefixes):
            raise AssertionError("Found string '%s' matching no expected prefix: %s"
                                 % (string, allowed_prefixes))


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

        # Since there is no central_server supplied, expect all resources to be served relatively.
        main_page_bs = BeautifulSoup(root_request.content, 'html.parser')
        main_page_link_hrefs = [l['href'] for l in main_page_bs.find_all('link')]
        main_page_script_srcs = [s['src'] for s in main_page_bs.find_all('script') if 'src' in s]
        page_resource_urls = main_page_link_hrefs + main_page_script_srcs
        assert_all_match_some_prefix(page_resource_urls, ['/', 'https://fonts.googleapis'])

        tasks_api_request = requests.get(flame_url + '/api/tasks')
        assert tasks_api_request.status_code == 200
        task_names = [t['name'] for t in tasks_api_request.json().values()]
        assert self.chain in task_names, "Task API endpoint didn't include executed chain (%s)." % self.chain


class FlameLaunchWithCentralServerTest(FlameFlowTestConfiguration):
    """Verifies Flame UI correctly includes central server when supplied from firexapp."""

    def __init__(self):
        self.central_server = 'http://some_server.com'
        self.central_server_ui_path = '/some/path'
        self.central_server_ui_url = self.central_server + self.central_server_ui_path
        super().__init__()

    def initial_firex_options(self) -> list:
        return ["submit", "--chain", 'nop', '--flame_central_server', self.central_server,
                '--flame_central_server_ui_path', self.central_server_ui_path]

    def assert_on_flame_url(self, log_dir, flame_url):
        root_request = requests.get(flame_url)
        assert root_request.ok, 'Expected OK response when fetching main page: %s' % root_request.status_code

        # Since there is no central_server supplied, expect all resources to be served relatively.
        main_page_bs = BeautifulSoup(root_request.content, 'html.parser')
        main_page_link_hrefs = [l['href'] for l in main_page_bs.find_all('link')]
        main_page_script_srcs = [s['src'] for s in main_page_bs.find_all('script') if 'src' in s]
        page_resource_urls = main_page_link_hrefs + main_page_script_srcs
        assert_all_match_some_prefix(page_resource_urls, [self.central_server_ui_url, 'https://fonts.googleapis'])


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
    return wait_until_tasks_predicate(lambda _, r: r is not None, log_dir, timeout, sleep_for)


def wait_until_task_name_exists(log_dir, task_name, timeout=20, sleep_for=1):
    return wait_until_tasks_predicate(lambda ts, _: any(t['name'] == task_name for t in ts.values()),
                                      log_dir, timeout, sleep_for)


def wait_until_tasks_predicate(tasks_pred, log_dir, timeout=20, sleep_for=1):
    def until_pred():
        if not os.path.isfile(get_rec_file(log_dir)):
            return False
        tasks, root_uuid = get_tasks_from_log_dir(log_dir)
        return tasks_pred(tasks, root_uuid)

    return wait_until(until_pred, timeout, sleep_for)


def get_tasks_by_name(log_dir, name, expect_single=False):
    tasks, _ = get_tasks_from_log_dir(log_dir)
    tasks_with_name = [t for t in tasks.values() if t['name'] == name]
    if expect_single and tasks_with_name:
        nameed_task_count = len(tasks_with_name)
        assert nameed_task_count == 1, "Expecting single task with name '%s', but found %s" % (name, nameed_task_count)
        return tasks_with_name[0]
    return tasks_with_name


class FlameRevokeNonExistantUuidTest(FlameFlowTestConfiguration):
    """ Uses Flame's SocketIO API to revoke a uuid that doesn't exist. """

    def initial_firex_options(self) -> list:
        return ["submit", "--chain", 'nop']

    def assert_on_flame_url(self, log_dir, flame_url):
        sio_client = socketio.Client()
        resp = {'response': None}
        FAILED_EVENT = 'revoke-failed'

        @sio_client.on('revoke-success')
        def revoke_success(_):
            resp['response'] = 'revoke-success'

        @sio_client.on(FAILED_EVENT)
        def revoke_failed(_):
            resp['response'] = FAILED_EVENT

        sio_client.connect(flame_url)
        sio_client.emit('revoke-task', data='This is not a UUID.')
        wait_until(lambda: resp['response'] is not None, timeout=15, sleep_for=1)
        sio_client.disconnect()

        assert resp['response'] == FAILED_EVENT, "Expected response %s but received %s" \
                                                 % (FAILED_EVENT, resp['response'])


#   Underlying revoke call (celery_app.control.revoke(uuid, terminate=True) doesn't work in GCP for some reason,
#   So this test can't be executed there.
#
@skip_test
class FlameRevokeSuccessTest(FlameFlowTestConfiguration):
    """ Uses Flame's SocketIO API to revoke a run. """

    # Don't run with --sync, since this test will revoke the incomplete root task.
    sync = False

    def initial_firex_options(self) -> list:
        # Sleep so that assert_on_flame_url can call revoke while the run is incomplete.
        return ["submit", "--chain", 'sleep', '--sleep', '90']

    def assert_on_flame_url(self, log_dir, flame_url):
        sleep_exists = wait_until_task_name_exists(log_dir, 'sleep')
        assert sleep_exists, "Sleep task doesn't exist in the flame rec file, something is wrong with run."
        sleep_task = get_tasks_by_name(log_dir, 'sleep', expect_single=True)
        assert sleep_task['state'] in INCOMPLETE_STATES, \
            "Expected incomplete sleep, but found %s" % sleep_task['state']

        sio_client = socketio.Client()
        resp = {'response': None}
        SUCCESS_EVENT = 'revoke-success'

        @sio_client.on(SUCCESS_EVENT)
        def revoke_success(_):
            resp['response'] = SUCCESS_EVENT

        @sio_client.on('revoke-failed')
        def revoke_failed(_):
            resp['response'] = 'revoke-failed'

        sio_client.connect(flame_url)
        sio_client.emit('revoke-task', data=sleep_task['uuid'])
        wait_until(lambda: resp['response'] is not None, timeout=60, sleep_for=1)
        sio_client.disconnect()

        assert resp['response'] == SUCCESS_EVENT, "Expected response %s but received %s" \
                                                  % (SUCCESS_EVENT, resp['response'])

        # Need to re-parse task data, since now it should be revoked.
        sleep_task_after_revoke = get_tasks_by_name(log_dir, 'sleep', expect_single=True)
        sleep_runstate = sleep_task_after_revoke['state']
        assert sleep_runstate == 'task-revoked', "Expected sleep runstate to be revoked, was %s" % sleep_runstate
