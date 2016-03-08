import abc
from ..systems import System
from ..config import Config


class Task(object):
    """
    A single task.
    It has 6 stages:
    1. check_pre_staging: Check if the task can be run in the first place, i.e.
       check if everything necessary is available.
    2. stage_data: Stage the data for the run. Only do quick things
       here...for long data copies and moves use a separate task.
    3. check_post_staging: Check if the staging was successful.
    4. run: Run the task.
    5. check_post_run: Check if the task has been successful.
    6. generate_next_steps: Generate the next task.
    If any stage raises, the task will not complete successfully.

    :type remote_machine: System
    :type config: Config
    """

    __metaclass__ = abc.ABCMeta

    def __init__(self, remote_machine, config):

        self.remote_machine = remote_machine
        self.config = config

    @abc.abstractmethod
    def check_pre_staging(self):
        pass

    @abc.abstractmethod
    def stage_data(self):
        pass

    @abc.abstractmethod
    def check_post_staging(self):
        pass

    @abc.abstractmethod
    def run(self):
        pass

    @abc.abstractmethod
    def check_post_run(self):
        pass