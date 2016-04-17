import io
import os

import click

from oval_office_2 import utilities
from oval_office_2.job_queue import JobQueue
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
        hpass = 1 / self.iteration_info['highpass']
        lpass = 1 / self.iteration_info['lowpass']
        pre_dir_string = 'preprocessed_{:.1f}_{:.1f}'.format(lpass, hpass)

        # Copy remote data.
        all_events = sorted(self.event_info.keys())
        lowpass_period = 1 / self.iteration_info["lowpass"]
        highpass_period = 1 / self.iteration_info["highpass"]
        self.remote_machine.makedir(self.config.window_dir)
        syn_base = os.path.join(self.config.lasif_project_path, "SYNTHETICS")
        dat_base = os.path.join(self.config.lasif_project_path, 'DATA')
        with click.progressbar(all_events, label="Copying data...") as events:
            for event in events:
                event_dir = os.path.join(self.config.window_dir, event)
                self.remote_machine.makedir(event_dir)

                syn_dat = os.path.join(syn_base, event,
                                       self.config.base_iteration, "synthetics.mseed")
                self.remote_machine.execute_command(
                    "rsync {} {}".format(syn_dat, event_dir))

                pro_dat = os.path.join(dat_base, event, pre_dir_string, 'preprocessed_data.mseed')
                self.remote_machine.execute_command(
                    'rsync {} {}'.format(pro_dat, event_dir))

        # Put Stations
        click.secho('Copying stations...')
        self.remote_machine.execute_command('rsync -a {} {}'.format(
            os.path.join(self.config.lasif_project_path, 'STATIONS', 'StationXML'),
            self.config.window_dir))

        # Put local script.
        remote_script = os.path.join(self.config.window_dir, "select_windows.py")
        with io.open(utilities.get_script_file("select_windows"), "r") as fh:
            script_string = fh.readlines()
        script_string.insert(0, "#!{}\n".format(self.config.python_exec))
        self.remote_machine.write_file(remote_script, "".join(script_string))

        # Put data
        file = "lasif_data.p"
        self.remote_machine.put_file(file, os.path.join(self.config.window_dir, file))

        # Put python script.
        remote_script = os.path.join(self.config.window_dir, 'select_windows.py')
        with io.open(utilities.get_script_file('select_windows'), 'r') as fh:
            script_string = fh.readlines()
        script_string.insert(0, '#!{}\n'.format(self.config.python_exec))
        self.remote_machine.write_file(remote_script, ''.join(script_string))

        # Sbatch
        self.sbatch_dict["python_exec"] = os.path.dirname(self.config.python_exec)
        remote_sbatch = os.path.join(self.config.window_dir, 'select_windows.sbatch')
        with io.open(utilities.get_template_file('sbatch_python_parallel'), 'r') as fh:
            sbatch_string = fh.read().format(**self.sbatch_dict)
        self.remote_machine.write_file(remote_sbatch, sbatch_string)


    def check_post_staging(self):
        pass

    def run(self):
        exec_command = 'chmod +x select_windows.py; sbatch select_windows.sbatch'
        queue = JobQueue(self.remote_machine, name="Select windows")
        _, so, _ = self.remote_machine.execute_command(
            exec_command, workdir=self.config.window_dir)
        queue.add_job(utilities.get_job_number_from_stdout(so))

        queue.flash_report(10)


    def check_post_run(self):
        all_events = sorted(self.event_info.keys())
        with click.progressbar(all_events, label="Saving windows.p's to LASIF...") as events:
            for event in events:

                try:
                    src_path = os.path.join(self.config.window_dir, event, 'windows.p')
                    target_dir = os.path.join(self.config.lasif_project_path,
                                              'ADJOINT_SOURCES_AND_WINDOWS/WINDOWS',
                                              self.config.base_iteration, event)
                    print src_path
                    print target_dir
                    self.remote_machine.makedir(target_dir)
                    self.remote_machine.execute_command(
                        "rsync {} {}".format(src_path, target_dir))
                except:
                    print '\n Could not find a windows.p to sync for: ' + event

