import os

import click

from oval_office_2 import utilities
from . import task

class SaveSynthetics(task.Task):

    def __init__(self, remote_machine, config):
        super(SaveSynthetics, self).__init__(remote_machine, config)
        self.event_info, self.iteration_info = utilities.get_lasif_information(
            self.remote_machine, self.config.lasif_project_path,
            self.config.base_iteration)
        self.processed_events = []

    def check_pre_staging(self):

        all_events = sorted(self.event_info.keys())
        with click.progressbar(all_events, label="Checking for synthetics...") as events:
            for event in events:
                output = os.path.join(self.config.solver_dir, event, "OUTPUT_FILES")
                files = self.remote_machine.ftp_connection.listdir(output)
                if "synthetics.mseed" in files:
                    self.processed_events.append(event)

    def stage_data(self):
        pass

    def check_post_staging(self):
        pass

    def run(self):

        base_src = os.path.join(self.config.solver_dir)
        base_dst = os.path.join(self.config.lasif_project_path, "SYNTHETICS")
        with click.progressbar(self.processed_events, label="Copying synthetics..") as events:
            for event in events:

                dst = os.path.join(base_dst, event, self.config.base_iteration)
                src = os.path.join(base_src, event, "OUTPUT_FILES", "synthetics.mseed")
                self.remote_machine.execute_command("rsync {} {}".format(src, dst))


    def check_post_run(self):
        pass
