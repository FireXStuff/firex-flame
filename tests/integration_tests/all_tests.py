import abc
import re
import os
import psutil
import signal
import time
import requests

from firexapp.testing.config_base import FlowTestConfiguration, assert_is_good_run
from firexapp.submit.submit import get_log_dir_from_output

from firex_flame.flame_helper import get_flame_pid


def flame_url_from_output(cmd_output):
    m = re.search(r'Flame: (http://.*)\n', cmd_output)
    if m:
        return m.group(1)
    return None


def kill_flame(log_dir, sig=signal.SIGKILL):
    flame_pid = get_flame_pid(log_dir)
    if psutil.pid_exists(flame_pid):
        os.kill(flame_pid, sig)
    return flame_pid


class FlameFlowTestConfiguration(FlowTestConfiguration):
    __metaclass__ = abc.ABCMeta

    def assert_expected_firex_output(self, cmd_output, cmd_err):
        log_dir = get_log_dir_from_output(cmd_output)
        flame_url = flame_url_from_output(cmd_output)
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
        return ["submit", "--chain", self.chain, '--sync']

    def assert_on_flame_url(self, log_dir, flame_url):
        assert get_flame_pid(log_dir) > 0, "Found no pid: %s" % get_flame_pid(log_dir)

        main_page_request = requests.get(flame_url)

        assert main_page_request.status_code == 200, \
            'Unexpected response code when fetching main page: %s' % main_page_request.status_code
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
        return ["submit", "--chain", 'nop', '--sync', '--flame_timeout', str(self.flame_timeout)]

    def assert_on_flame_url(self, log_dir, flame_url):
        time.sleep(self.flame_timeout + 1)
        flame_pid = get_flame_pid(log_dir)
        assert not psutil.pid_exists(flame_pid), "Timeout should have killed flame pid, but it still exists."


class FlameSigtermShutdownTest(FlameFlowTestConfiguration):
    """ Flame shutsdown cleanly when getting a sigterm."""

    def initial_firex_options(self) -> list:
        return ["submit", "--chain", 'nop', '--sync']

    def assert_on_flame_url(self, log_dir, flame_url):
        flame_pid = get_flame_pid(log_dir)
        assert psutil.pid_exists(flame_pid), "Flame pid should exist before being killed by SIGTERM."
        kill_flame(log_dir, sig=signal.SIGTERM)
        time.sleep(2)
        assert not psutil.pid_exists(flame_pid), "SIGTERM should have caused flame to terminate, but pid still exists."


class FlameSigintShutdownTest(FlameFlowTestConfiguration):
    """ Flame shutsdown cleanly when getting a sigint."""

    def initial_firex_options(self) -> list:
        return ["submit", "--chain", 'nop', '--sync']

    def assert_on_flame_url(self, log_dir, flame_url):
        flame_pid = get_flame_pid(log_dir)
        assert psutil.pid_exists(flame_pid), "Flame pid should exist before being killed by SIGINT."
        kill_flame(log_dir, sig=signal.SIGINT)
        time.sleep(2)
        assert not psutil.pid_exists(flame_pid), "SIGINT should have caused flame to terminate, but pid still exists."
