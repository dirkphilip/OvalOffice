import click
import os

from . import task
from .. import utilities


class SetupSpecfemDirectories(task.Task):
    """Sets up the necessary directory structures for a specfem run."""

    def __init__(self, remote_machine, config):
        super(SetupSpecfemDirectories, self).__init__(remote_machine, config)
        self.event_info, self.iteration_info = utilities.get_lasif_information(
            self.remote_machine, self.config.lasif_project_path, self.config.base_iteration)

    def check_pre_staging(self):
        pass

    def stage_data(self):

        pass

    def check_post_staging(self):
        pass

    def run(self):

        req_specfem_dirs = ['bin', 'DATA', 'DATA/GLL', 'DATA/cemRequest', 'DATA/topo_bathy',
                            'OUTPUT_FILES', 'DATABASES_MPI', 'SEM']

        with click.progressbar(sorted(self.event_info.keys()) + ['MESH'],
                               label="Creating directories...") as events:
            for event in events:
                event_dir = os.path.join(self.config.solver_dir, event)
                self.remote_machine.makedir(event_dir)
                for specfem_dir in req_specfem_dirs:
                    sub_dir = os.path.join(event_dir, specfem_dir)
                    self.remote_machine.makedir(sub_dir)

    def check_post_run(self):
        pass
