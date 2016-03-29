import io
import os
import click

from oval_office_2 import utilities
from oval_office_2.job_queue import JobQueue
from . import task

class PreprocessData(task.Task):
    """Runs the LASIF provided preprocessing scripts on raw data."""

    def __init__(self, remote_machine, config, sbatch_dict):
        super(PreprocessData, self).__init__(remote_machine, config)
        self.event_info, self.iteration_info = utilities.get_lasif_information(
            self.remote_machine, self.config.lasif_project_path,
            self.config.base_iteration)

        self.all_events = sorted(self.event_info.keys())
        self.sbatch_dict = sbatch_dict

    def check_pre_staging(self):

        no_data = []
        with click.progressbar(self.all_events, label="Checking for raw data...") as events:
            for event in events:

                raw_dir = os.path.join(self.config.lasif_project_path, 'DATA', event, 'raw')
                files = self.remote_machine.ftp_connection.listdir(raw_dir)
                if 'data.mseed' not in files:
                    no_data.append(event)


        if no_data:
            print("No raw data found for events\n", ", ".join(no_data))


    def stage_data(self):

        self.remote_machine.makedir(self.config.preprocessing_dir)
        click.secho('Copying stations...')
        self.remote_machine.execute_command('rsync -a {} {}'.format(
            os.path.join(self.config.lasif_project_path, 'STATIONS', 'StationXML'),
            self.config.preprocessing_dir))
        with click.progressbar(self.all_events, label="Copying data...") as events:
            for event in events:
                raw_dir = os.path.join(self.config.lasif_project_path, 'DATA', event, 'raw', 'data.mseed')
                event_dir = os.path.join(self.config.preprocessing_dir, event)
                self.remote_machine.makedir(event_dir)
                self.remote_machine.execute_command('rsync {} {}'.format(raw_dir, event_dir))

        self.remote_machine.put_file('lasif_data.p',
                                     os.path.join(self.config.preprocessing_dir, 'lasif_data.p'))

        remote_script = os.path.join(self.config.preprocessing_dir, "preprocess_data.py")
        with io.open(utilities.get_script_file('preprocess_data'), 'r') as fh:
            script_string = fh.readlines()
        script_string.insert(0, '#!{}\n'.format(self.config.python_exec))
        self.remote_machine.write_file(remote_script, ''.join(script_string))

        remote_sbatch = os.path.join(self.config.preprocessing_dir, 'preprocess_data.sbatch')
        with io.open(utilities.get_template_file('sbatch'), 'r') as fh:
            sbatch_string = fh.read().format(**self.sbatch_dict)
        self.remote_machine.write_file(remote_sbatch, sbatch_string)

    def check_post_staging(self):
        pass

    def run(self):

        exec_command = 'chmod +x preprocess_data.py; sbatch preprocess_data.sbatch'
        queue = JobQueue(self.remote_machine, name="Preprocess data")
        _, so, _ = self.remote_machine.execute_command(
            exec_command, workdir=self.config.preprocessing_dir)
        queue.add_job(utilities.get_job_number_from_stdout(so))

        queue.flash_report(10)


    def check_post_run(self):
        pass



