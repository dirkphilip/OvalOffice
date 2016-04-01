import io
import os
import click

from oval_office_2 import utilities
from oval_office_2.job_queue import JobQueue
from . import task

class createAdjointSources(task.Task):
    """Runs the LASIF provided create_adjoint_sources script on preprocessed and
     synthetic data with the help of a window selection pickle file.
    ."""

    def __init__(self, remote_machine, config, sbatch_dict):
        super(createAdjointSources, self).__init__(remote_machine, config)
        self.event_info, self.iteration_info = utilities.get_lasif_information(
            self.remote_machine, self.config.lasif_project_path,
            self.config.base_iteration)

        self.all_events = sorted(self.event_info.keys())
        self.sbatch_dict = sbatch_dict

    def check_pre_staging(self):
        # preproc data check
        no_data = []
        with click.progressbar(self.all_events, label="Checking for preprocessed data...") as events:
            for event in events:
                raw_dir = os.path.join(self.config.lasif_project_path, 'DATA', event, 'preprocessed_50.0_100.0')
                files = self.remote_machine.ftp_connection.listdir(raw_dir)
                if 'preprocessed_data.mseed' not in files:
                    no_data.append(event)

        if no_data:
            print("No preprocessed data found for events\n", ", ".join(no_data))

        no_data = []
        with click.progressbar(self.all_events, label="Checking for synthetics...") as events:
            for event in events:
                raw_dir = os.path.join(self.config.lasif_project_path, 'SYNTHETICS', event, self.config.base_iteration)
                files = self.remote_machine.ftp_connection.listdir(raw_dir)
                if 'synthetics.mseed' not in files:
                    no_data.append(event)

        if no_data:
            print("No synthetics found for events\n", ", ".join(no_data))

        no_data = []
        with click.progressbar(self.all_events, label="Checking for windows...") as events:
            for event in events:
                raw_dir = os.path.join(self.config.lasif_project_path, 'ADJOINT_SOURCES_AND_WINDOWS/WINDOWS', self.config.base_iteration, event)
                files = self.remote_machine.ftp_connection.listdir(raw_dir)
                if 'windows.p' not in files:
                    no_data.append(event)

        if no_data:
            print("No windows found for events\n", ", ".join(no_data))


    def stage_data(self):
        self.remote_machine.makedir(self.config.adjoint_dir)

        with click.progressbar(self.all_events, label="Copying preprocessed data...") as events:
            for event in events:
                raw_dir = os.path.join(self.config.lasif_project_path, 'DATA', event, 'preprocessed_50.0_100.0', 'preprocessed_data.mseed')
                event_dir = os.path.join(self.config.adjoint_dir, event)
                self.remote_machine.makedir(event_dir)
                self.remote_machine.execute_command('rsync {} {}'.format(raw_dir, event_dir))

        with click.progressbar(self.all_events, label="Copying synthetics...") as events:
            for event in events:
                raw_dir = os.path.join(self.config.lasif_project_path, 'SYNTHETICS', event, self.config.base_iteration, 'synthetics.mseed')
                event_dir = os.path.join(self.config.adjoint_dir, event)
                self.remote_machine.execute_command('rsync {} {}'.format(raw_dir, event_dir))

        with click.progressbar(self.all_events, label="Copying windows...") as events:
            for event in events:
                raw_dir = os.path.join(self.config.lasif_project_path, 'ADJOINT_SOURCES_AND_WINDOWS/WINDOWS', self.config.base_iteration ,event, 'windows.p')
                event_dir = os.path.join(self.config.adjoint_dir, event)
                self.remote_machine.execute_command('rsync {} {}'.format(raw_dir, event_dir))

        self.remote_machine.put_file('lasif_data.p',
                                     os.path.join(self.config.adjoint_dir, 'lasif_data.p'))

        remote_script = os.path.join(self.config.adjoint_dir, "create_adjoint_sources.py")
        with io.open(utilities.get_script_file('create_adjoint_sources'), 'r') as fh:
            script_string = fh.readlines()
        script_string.insert(0, '#!{}\n'.format(self.config.python_exec))
        self.remote_machine.write_file(remote_script, ''.join(script_string))

        remote_sbatch = os.path.join(self.config.adjoint_dir, 'create_adjoint_sources.sbatch')
        with io.open(utilities.get_template_file('sbatch'), 'r') as fh:
            sbatch_string = fh.read().format(**self.sbatch_dict)
        self.remote_machine.write_file(remote_sbatch, sbatch_string)

    def check_post_staging(self):
        pass

    def run(self):

        exec_command = 'chmod +x create_adjoint_sources.py; sbatch create_adjoint_sources.sbatch'
        queue = JobQueue(self.remote_machine, name="Create Adjoint Sources")
        _, so, _ = self.remote_machine.execute_command(
            exec_command, workdir=self.config.adjoint_dir)
        queue.add_job(utilities.get_job_number_from_stdout(so))

        queue.flash_report(10)
        print " hey man"

    def check_post_run(self):
        pass



