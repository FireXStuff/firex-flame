import abc
import re
import os
import signal
import requests

from firexapp.testing.config_base import FlowTestConfiguration, assert_is_good_run
from firexapp.submit.submit import get_log_dir_from_output

from firex_flame.flame_helper import get_flame_pid


def flame_url_from_output(cmd_output):
    m = re.search(r'Flame: (http://.*)\n', cmd_output)
    if m:
        return m.group(1)
    return None


class FlameFlowTestConfiguration(FlowTestConfiguration):
    __metaclass__ = abc.ABCMeta

    def assert_expected_firex_output(self, cmd_output, cmd_err):
        log_dir = get_log_dir_from_output(cmd_output)
        flame_url = flame_url_from_output(cmd_output)
        self.assert_on_flame_url(log_dir, flame_url)
        # do teardown here until there is a real teardown method.
        try:
            os.kill(get_flame_pid(log_dir), signal.SIGKILL)
            print('killed flame pid.')
        except OSError:
            # Tests might have already killed the run, so an exception b/c it doesn't exist is OK.
            pass

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
        return ["submit", "--chain", self.chain]  # "--flame_timeout", "120"

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
    """ TODO: Flame shuts itself down when a given timeout is exceeded."""
    pass
