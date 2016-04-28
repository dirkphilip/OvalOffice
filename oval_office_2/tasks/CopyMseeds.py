import os

import boltons.fileutils

from oval_office_2 import utilities
from . import task
import click



class CopyMseeds(task.Task):
    """Copies compiled binaries to relevant scratch directories.

    For each SPECFEM3D_GLOBE run, compiled solver binaries, along with
    information regarding topography, etc., are required in the run directory.
    This function copies all relevant files from the specfem source directory.
    """

    def __init__(self, remote_machine, config):
        super(CopyMseeds, self).__init__(remote_machine, config)
        self.event_info, self.iteration_info = utilities.get_lasif_information(
            self.remote_machine, self.config.lasif_project_path,
            self.config.base_iteration)
        self.getWindows = True

    def check_pre_staging(self):
        pass

    def stage_data(self):
        boltons.fileutils.mkdir_p("PREPROC_DATA")
        boltons.fileutils.mkdir_p("SYNTHETICS")
        boltons.fileutils.mkdir_p("WINDOWS")

    def check_post_staging(self):
        pass

    def run(self):
        all_events = sorted(self.event_info.keys())
        with click.progressbar(all_events, label="Copying preprocessed data ...") as events:
            for event in events:
                raw_dir = os.path.join(self.config.lasif_project_path, "DATA",
                                       event, "preprocessed_50.0_100.0/")
                filename = raw_dir + "preprocessed_data.mseed"

                event_dir = os.path.join("PREPROC_DATA", event, self.config.base_iteration)
                boltons.fileutils.mkdir_p(event_dir)

                event_data = os.path.join(event_dir, "preprocessed_data.mseed".format(event))

                self.remote_machine.get_rsync(filename,event_data, verbose=False)

        with click.progressbar(all_events, label="Copying synthetics ...") as events:
            for event in events:
                raw_dir = os.path.join(self.config.lasif_project_path, "SYNTHETICS",
                                       event, self.config.base_iteration)
                filename = raw_dir + "/synthetics.mseed"
                event_dir = os.path.join("SYNTHETICS", event, self.config.base_iteration)
                boltons.fileutils.mkdir_p(event_dir)

                event_data = os.path.join(event_dir, "synthetics.mseed".format(event))

                self.remote_machine.get_rsync(filename,event_data, verbose=False)

        if self.getWindows:
            with click.progressbar(all_events, label="Copying windows ...") as events:
                for event in events:
                    raw_dir = os.path.join(self.config.lasif_project_path, "ADJOINT_SOURCES_AND_WINDOWS/WINDOWS",
                                           self.config.first_iteration, event)

                    filename = raw_dir + "/windows.p"

                    event_dir = os.path.join("WINDOWS", event, self.config.base_iteration)
                    boltons.fileutils.mkdir_p(event_dir)

                    event_data = os.path.join(event_dir, "windows.p".format(event))

                    self.remote_machine.get_rsync(filename,event_data, verbose=False)

    def check_post_run(self):
        pass
