import os

import click

from oval_office_2 import utilities
from . import task


class CopyBinariesToRunDirectory(task.Task):
    """Copies compiled binaries to relevant scratch directories.

    For each SPECFEM3D_GLOBE run, compiled solver binaries, along with
    information regarding topography, etc., are required in the run directory.
    This function copies all relevant files from the specfem source directory.
    """

    def __init__(self, remote_machine, config):
        super(CopyBinariesToRunDirectory, self).__init__(remote_machine, config)
        self.event_info, self.iteration_info = utilities.get_lasif_information(
            self.remote_machine, self.config.lasif_project_path,
            self.config.base_iteration)

    def check_pre_staging(self):
        pass

    def stage_data(self):
        pass

    def check_post_staging(self):
        pass

    def run(self):
        bins = os.path.join(self.config.specfem_src_dir, 'bin', '*fem*')
        topo = os.path.join(self.config.specfem_src_dir, 'DATA', 'topo_bathy')
        all_events = sorted(self.event_info.keys()) + ['MESH']
        with click.progressbar(all_events, label="Copying binaries...") as events:
            for event in events:

                # Copy binaries
                event_bin = os.path.join(self.config.solver_dir, event, 'bin')
                cmd = "rsync {} {}".format(bins, event_bin)
                self.remote_machine.execute_command(cmd)

                # Copy topo_bathy
                event_topo = os.path.join(self.config.solver_dir, event, 'DATA')
                cmd = "rsync -a {} {}".format(topo, event_topo)
                self.remote_machine.execute_command(cmd)

    def check_post_run(self):
        pass
