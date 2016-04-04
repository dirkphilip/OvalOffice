import io
import os

from oval_office_2 import utilities
from oval_office_2.job_queue import JobQueue
from . import task

class writeAdjointSources(task.Task):
    """Runs the LASIF provided write_adjoint_sources script on adjoint_sources.p and
     synthetic data with the help of a window selection pickle file.
    ."""

    def __init__(self, remote_machine, config, sbatch_dict):
        super(writeAdjointSources, self).__init__(remote_machine, config)
        self.event_info, self.iteration_info = utilities.get_lasif_information(
            self.remote_machine, self.config.lasif_project_path,
            self.config.base_iteration)

        self.all_events = sorted(self.event_info.keys())
        self.sbatch_dict = sbatch_dict

    def check_pre_staging(self):
        pass

    def stage_data(self):

        # Get adjoint_sources.p
        raw_dir = os.path.join(self.config.lasif_project_path,  'ADJOINT_SOURCES_AND_WINDOWS/ADJOINT_SOURCES', self.config.base_iteration, 'adjoint_sources.p')
        event_dir = os.path.join(self.config.solver_dir)
        self.remote_machine.execute_command('rsync {} {}'.format(raw_dir, event_dir))

        # Get lasif_data.p
        raw_dir = os.path.join(self.config.lasif_project_path,  'lasif_data.p')
        event_dir = os.path.join(self.config.solver_dir)
        self.remote_machine.execute_command('rsync {} {}'.format(raw_dir, event_dir))

        remote_script = os.path.join(self.config.solver_dir, "write_adjoint_sources.py")
        with io.open(utilities.get_script_file('write_adjoint_sources'), 'r') as fh:
            script_string = fh.readlines()
        script_string.insert(0, '#!{}\n'.format(self.config.python_exec))
        self.remote_machine.write_file(remote_script, ''.join(script_string))

        remote_sbatch = os.path.join(self.config.solver_dir, 'write_adjoint_sources.sbatch')
        with io.open(utilities.get_template_file('sbatch'), 'r') as fh:
            sbatch_string = fh.read().format(**self.sbatch_dict)
        self.remote_machine.write_file(remote_sbatch, sbatch_string)

    def check_post_staging(self):
        pass

    def run(self):

        exec_command = 'chmod +x write_adjoint_sources.py; sbatch write_adjoint_sources.sbatch'
        queue = JobQueue(self.remote_machine, name="Write Adjoint Sources")
        _, so, _ = self.remote_machine.execute_command(
            exec_command, workdir=self.config.solver_dir)
        queue.add_job(utilities.get_job_number_from_stdout(so))

        queue.flash_report(10)

    def check_post_run(self):
        pass



