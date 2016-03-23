import os

from oval_office_2 import utilities
from . import task
import click


class CopyRawData(task.Task):
    """Copies compiled binaries to relevant scratch directories.

    For each SPECFEM3D_GLOBE run, compiled solver binaries, along with
    information regarding topography, etc., are required in the run directory.
    This function copies all relevant files from the specfem source directory.
    """

    def __init__(self, remote_machine, config):
        super(CopyRawData, self).__init__(remote_machine, config)
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
        all_events = sorted(self.event_info.keys())
        with click.progressbar(all_events, label="Copying data files ...") as events:
            for event in events:
                raw_dir = os.path.join(self.config.lasif_project_path, "DATA",
                                       event, "raw/")
                filename = raw_dir + "data.mseed"
                event_data = os.path.join("RAW_DATA", "{}.mseed".format(event))

                self.remote_machine.makedir(raw_dir)
                self.remote_machine.put_rsync(event_data, filename, verbose=True)

    def check_post_run(self):
        pass
