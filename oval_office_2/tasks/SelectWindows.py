import os

import click
import io

from oval_office_2 import utilities
from . import task

class SelectWindows(task.Task):

    def __init__(self, remote_machine, config, sbatch_dict):

        super(SelectWindows, self).__init__(remote_machine, config)
        self.event_info, self.iteration_info = utilities.get_lasif_information(
            self.remote_machine, self.config.lasif_project_path,
            self.config.base_iteration)
        self.sbatch_dict = sbatch_dict

    def check_pre_staging(self):
        pass

    def stage_data(self):

        # Copy remote data.
        # all_events = sorted(self.event_info.keys())
        # lowpass_period = 1 / self.iteration_info["lowpass"]
        # highpass_period = 1 / self.iteration_info["highpass"]
        # self.remote_machine.makedir(self.config.window_dir)
        # syn_base = os.path.join(self.config.lasif_project_path, "SYNTHETICS")
        # with click.progressbar(all_events, label="Copying data...") as events:
        #     for event in events:
        #
        #         event_dir = os.path.join(self.config.window_dir, event)
        #         self.remote_machine.makedir(event_dir)
        #
        #         syn_dat = os.path.join(syn_base, event,
        #                                self.config.base_iteration, "synthetics.mseed")
        #         self.remote_machine.execute_command(
        #             "rsync {} {}".format(syn_dat, event_dir))

        # Put local script.
        remote_script = os.path.join(self.config.window_dir, "select_windows.py")
        with io.open(utilities.get_script_file("select_windows"), "r") as fh:
            script_string = fh.readlines()
        script_string.insert(0, "#!{}\n".format(self.config.python_exec))
        self.remote_machine.write_file(remote_script, "".join(script_string))

        # Put data
        file = "lasif_data.p"
        self.remote_machine.put_file(file, os.path.join(self.config.window_dir, file))


    def check_post_staging(self):
        pass

    def run(self):
        pass

    def check_post_run(self):
        pass




