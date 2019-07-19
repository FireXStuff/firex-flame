import abc
import re
import os
import json
import psutil
import signal
import time
import requests
import urllib.parse


from bs4 import BeautifulSoup
import socketio
from firexapp.testing.config_base import FlowTestConfiguration, assert_is_good_run
from firexapp.submit.submit import get_log_dir_from_output
from firexapp.engine.celery import app
from firexkit.chain import returns

from firex_flame.event_file_processor import get_tasks_from_rec_file
from firex_flame.flame_helper import get_flame_pid, wait_until_pid_not_exist, wait_until, get_rec_file, filter_paths
from firex_flame.event_aggregator import INCOMPLETE_STATES, COMPLETE_STATES
from firex_flame.model_dumper import get_tasks_slim_file, get_model_full_tasks_by_names, is_dump_complete


def flame_url_from_output(cmd_output):
    m = re.search(r'Flame: (http://.*)\n', cmd_output)
    if m:
        return m.group(1)
    return None


def kill_flame(log_dir, sig=signal.SIGKILL):
    flame_pid = get_flame_pid(log_dir)
    kill_and_wait(flame_pid, sig)
    return flame_pid


def kill_and_wait(pid, sig=signal.SIGKILL):
    if psutil.pid_exists(pid):
        os.kill(pid, sig)
        wait_until_pid_not_exist(pid, timeout=10)
    return not psutil.pid_exists(pid)


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


def assert_flame_web_ok(flame_url, path):
    alive_url = urllib.parse.urljoin(flame_url, path)
    alive_request = requests.get(alive_url)
    assert alive_request.ok, 'Expected OK response when fetching %s resource page: %s' % (alive_url, path)


class FlameLaunchesTest(FlameFlowTestConfiguration):
    """Verifies Flame is launched with nop chain by requesting a couple web endpoints."""

    def __init__(self):
        self.chain = 'nop'
        super().__init__()

    def initial_firex_options(self) -> list:
        return ["submit", "--chain", self.chain]

    def assert_on_flame_url(self, log_dir, flame_url):
        assert get_flame_pid(log_dir) > 0, "Found no pid: %s" % get_flame_pid(log_dir)
        assert_flame_web_ok(flame_url, '/alive')

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
    return wait_until_rec_tasks_predicate(lambda _, r: r is not None, log_dir, timeout, sleep_for)


def wait_until_task_name_exists_in_rec(log_dir, task_name, timeout=20, sleep_for=1):
    return wait_until_rec_tasks_predicate(lambda ts, _: any(t['name'] == task_name for t in ts.values()),
                                          log_dir, timeout, sleep_for)


def wait_until_model_task_uuid_complete_runstate(log_dir, timeout=20, sleep_for=1):
    return wait_until_complete_model_tasks_predicate(
        lambda ts: all(t['state'] in COMPLETE_STATES for t in ts.values()), log_dir, timeout, sleep_for)


def read_json(file):
    with open(file) as fp:
            return json.load(fp)


def wait_until_complete_model_tasks_predicate(tasks_pred, log_dir, timeout=20, sleep_for=1):
    def until_pred():
        slim_model_file = get_tasks_slim_file(log_dir)
        if not os.path.isfile(slim_model_file):
            return False
        try:
            tasks_by_uuid = read_json(slim_model_file)
        except json.decoder.JSONDecodeError:
            return False
        else:
            return tasks_pred(tasks_by_uuid)

    return wait_until(until_pred, timeout, sleep_for)


def wait_until_rec_tasks_predicate(tasks_pred, log_dir, timeout=20, sleep_for=1):
    def until_pred():
        if not os.path.isfile(get_rec_file(log_dir)):
            return False
        tasks, root_uuid = get_tasks_from_rec_file(log_dir)
        return tasks_pred(tasks, root_uuid)

    return wait_until(until_pred, timeout, sleep_for)


def get_tasks_by_name(log_dir, name, expect_single=False):
    tasks, _ = get_tasks_from_rec_file(log_dir)
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


class FlameRevokeSuccessTest(FlameFlowTestConfiguration):
    """ Uses Flame's SocketIO API to revoke a run. """

    # Don't run with --sync, since this test will revoke the incomplete root task.
    sync = False
    no_coverage = True

    def initial_firex_options(self) -> list:
        # Sleep so that assert_on_flame_url can call revoke while the run is incomplete.
        return ["submit", "--chain", 'sleep', '--sleep', '90']

    def assert_on_flame_url(self, log_dir, flame_url):
        sleep_exists = wait_until_task_name_exists_in_rec(log_dir, 'sleep')
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
        wait_until(lambda: resp['response'] is not None, timeout=30, sleep_for=1)
        sio_client.disconnect()

        assert resp['response'] == SUCCESS_EVENT, "Expected response %s but received %s" \
                                                  % (SUCCESS_EVENT, resp['response'])

        # Need to re-parse task data, since now it should be revoked.
        sleep_task_after_revoke = get_tasks_by_name(log_dir, 'sleep', expect_single=True)
        sleep_runstate = sleep_task_after_revoke['state']
        assert sleep_runstate == 'task-revoked', "Expected sleep runstate to be revoked, was %s" % sleep_runstate


class FlameRedisKillCleanupTest(FlameFlowTestConfiguration):
    """ Kills redis and confirms flame kludges run states to be terminal """

    # Don't run with --sync, since this test will revoke the incomplete root task.
    sync = False

    def initial_firex_options(self) -> list:
        # Sleep so that assert_on_flame_url can kill redis and verify the data is clean up.
        return ["submit", "--chain", 'sleep', '--sleep', '30', '--broker_max_retry_attempts', '2']

    def assert_on_flame_url(self, log_dir, flame_url):
        sleep_exists = wait_until_task_name_exists_in_rec(log_dir, 'sleep')
        assert sleep_exists, "Sleep task doesn't exist in the flame rec file, something is wrong with run."

        from pathlib import Path
        redis_pid = int(Path(log_dir,'debug', 'redis', 'redis.pid').read_text())
        redis_killed = kill_and_wait(redis_pid, sig=signal.SIGKILL)
        assert redis_killed, "Failed to kill redis with pid %s" % redis_pid

        wait_until_model_task_uuid_complete_runstate(log_dir)

        runstates = [t['state'] for t in read_json(get_tasks_slim_file(log_dir)).values()]
        assert all(s == 'task-incomplete' for s in runstates), \
            "Expected all tasks incomplete, but found %s" % runstates

        flame_killed = wait_until_pid_not_exist(get_flame_pid(log_dir), timeout=4)
        assert not flame_killed, 'Flame killed after redis shutdown -- it should survive.'


class FlameTerminateOnCompleteTest(FlameFlowTestConfiguration):
    """ Tests 'terminate_on_complete' argument """

    # No sync since we'll revoke the run & confirm the web server is no longer active.
    sync = False
    no_coverage = True

    def initial_firex_options(self) -> list:
        return ["submit", "--chain", 'sleep', '--sleep', '30', '--flame_terminate_on_complete', 'True',
                # Tests timeout is ignored when terminate_on_complete is set.
                '--flame_timeout', '1']

    def assert_on_flame_url(self, log_dir, flame_url):
        root_task_exists = wait_until_task_name_exists_in_rec(log_dir, 'RootTask')
        assert root_task_exists, "Root task doesn't exist in the flame rec file, something is wrong with run."

        # Make sure the web API is stil up after the timeout, since it should be ignored due to terminate_on_complete.
        time.sleep(1)
        assert_flame_web_ok(flame_url, '/alive')

        root_task = get_tasks_by_name(log_dir, 'RootTask', expect_single=True)
        assert_flame_web_ok(flame_url, '/api/revoke/' + root_task['uuid'])

        # Somewhat big timeout since we need to wait for redis to shutdown gracefully, which then causes the
        # the broker processor to shutdown flame gracefully.
        flame_killed = wait_until_pid_not_exist(get_flame_pid(log_dir), timeout=30)
        assert flame_killed, 'Flame not terminated after root revoked, even though terminate_on_complete was supplied.'


# noinspection PyUnusedLocal
@app.task
@returns('sum')
def add(op1, op2):
    return int(op1) + int(op2)


class DumpDataOnCompleteTest(FlameFlowTestConfiguration):
    """ Tests task data model dumping performed at end of broker receiver. """

    def initial_firex_options(self) -> list:
        return ["submit", "--chain", 'add', '--op1', '1', '--op2', '2']

    def assert_on_flame_url(self, log_dir, flame_url):
        assert is_dump_complete(log_dir), "Model dump not complete, can't assert on task data."

        tasks_by_name = get_model_full_tasks_by_names(log_dir, ['add'])
        assert len(tasks_by_name) == 1
        assert len(tasks_by_name['add']) == 1

        query = {
            ('state',): 'task-succeeded',
            # TODO: add additional criteria when these fields are ported from firex legacy.
            # ('firex_bound_args', 'op1'): '1',
            # ('firex_bound_args', 'op2'): '2',
            # ('firex_result', 'sum'): 3,
        }

        found_paths = filter_paths(tasks_by_name, query)['add']
        print(found_paths)
        assert len(found_paths) == 1
