import io
import os

import click

from oval_office_2 import utilities
from oval_office_2.job_queue import JobQueue
from . import task


class AddSmoothedGradient(task.Task):

    def __init__(self, remote_machine, config, sbatch_dict, perturbation_percent):

        super(AddSmoothedGradient, self).__init__(remote_machine, config)
        self.event_info, self.iteration_info = utilities.get_lasif_information(
            self.remote_machine, self.config.lasif_project_path,
            self.config.base_iteration)
        self.sbatch_dict = sbatch_dict
        self.perturbation_percent = perturbation_percent

    def check_pre_staging(self):
        pass

    def stage_data(self):
        self.sbatch_dict['execute'] = 'aprun -B ./bin/xadd_model_tiso {:f}'.format(self.perturbation_percent)
        remote_sbatch = os.path.join(self.config.optimization_dir, 'add_smoothed_gradient.sbatch')
        with io.open(utilities.get_template_file('sbatch'), 'r') as fh:
            sbatch_string = fh.read().format(**self.sbatch_dict)
        self.remote_machine.write_file(remote_sbatch, sbatch_string)


    def check_post_staging(self):
        pass

    def run(self):
        exec_command = 'sbatch add_smoothed_gradient.sbatch'
        queue = JobQueue(self.remote_machine, name="add_smoothed_gradient")
        _, so, _ = self.remote_machine.execute_command(
            exec_command, workdir=self.config.optimization_dir)
        queue.add_job(utilities.get_job_number_from_stdout(so))

        queue.flash_report(10)

    def check_post_run(self):
        pass