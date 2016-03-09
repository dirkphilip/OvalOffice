import os

import click

from oval_office_2 import utilities
from . import task


class LinkMesh(task.Task):
    """Symlinks the mesh DATABASES_MPI directory to all event directories."""

    def __init__(self, remote_machine, config):
        super(LinkMesh, self).__init__(remote_machine, config)
        self.event_info, self.iteration_info = utilities.get_lasif_information(
            self.remote_machine, self.config.lasif_project_path,
            self.config.base_iteration)

    def check_pre_staging(self):
        pass

    def stage_data(self):
        pass

    def check_post_staging(self):

        root_dir = os.path.join(self.config.solver_dir, "MESH", "DATABASES_MPI")
        files = self.remote_machine.ftp_connection.listdir(root_dir)
        num_reg1 = sum("reg1_solver_data" in f for f in files)
        num_reg2 = sum("reg2_solver_data" in f for f in files)
        num_reg3 = sum("reg3_solver_data" in f for f in files)
        try:
            assert num_reg1 == num_reg2 == num_reg3
        except:
            raise RuntimeError("Some issue with mesh file generation. "
                               "Please check mesher")

    def run(self):

        root_dir = os.path.join(self.config.solver_dir, "MESH", "DATABASES_MPI", "*")
        root_out = os.path.join(self.config.solver_dir, "MESH", "OUTPUT_FILES", "*")
        all_events = sorted(self.event_info.keys())
        with click.progressbar(all_events, label="Symlinking mesh...") as events:
            for event in events:
                dest_dir = os.path.join(self.config.solver_dir, event, "DATABASES_MPI")
                dest_out = os.path.join(self.config.solver_dir, event, "OUTPUT_FILES")
                self.remote_machine.execute_command(
                    "ln -sf {} {}".format(root_dir, dest_dir))
                self.remote_machine.execute_command(
                    "ln -sf {} {}".format(root_out, dest_out))

    def check_post_run(self):
        pass
