import io
import os

import click

from oval_office_2 import utilities
from oval_office_2.job_queue import JobQueue
from . import task


class SumGradients(task.Task):
    def __init__(self, remote_machine, config, sbatch_dict):
        super(SumGradients, self).__init__(remote_machine, config)
        self.sbatch_dict = sbatch_dict
        self.complete_events = []
        self.event_info, self.iteration_info = utilities.get_lasif_information(
            self.remote_machine, self.config.lasif_project_path,
            self.config.base_iteration)

        self.all_events = sorted(self.event_info.keys())

    def check_pre_staging(self):

        all_kernels = ['alphah_kernel', 'alpha_kernel', 'alphav_kernel', 'betah_kernel', 'beta_kernel', 'betav_kernel',
                       'bulk_betah_kernel', 'bulk_beta_kernel', 'bulk_betav_kernel', 'bulk_c_kernel', 'eta_kernel',
                       'hess_kernel', 'rho_kernel']

        alt_solver_dirpath = self.config.solver_dir
        with click.progressbar(self.all_events, label='Finding kernels...') as events:
            for event in events:
                use_event = True
                event_dir = os.path.join(alt_solver_dirpath, event, 'DATABASES_MPI')
                files = self.remote_machine.ftp_connection.listdir(event_dir)
                for kernel in all_kernels:
                    num_k = len([x for x in files if kernel in x])
                    if num_k < 6 * int(self.config.nproc_eta) * int(self.config.nproc_xi):
                        use_event = False

                if use_event:
                    self.complete_events.append(event)

        print("--------------------")
        print("   FAILED EVENTS    ")
        print("--------------------")
        f_events = set(self.all_events) - set(self.complete_events)
        print('\n'.join(f_events))

    def stage_data(self):

        # Setup optimization dir_tree.
        directories = ['bin', 'PROCESSED_KERNELS', 'GRADIENT_INFO',
                       'LOGS', 'DATA', 'VTK_FILES']

        for d in directories:
            full_path = os.path.join(self.config.optimization_dir, d)
            self.remote_machine.makedir(full_path)

        # Get full path to complete events.
        alt_solver_dirpath = self.config.solver_dir
        full_events = [os.path.join(alt_solver_dirpath, event, 'DATABASES_MPI') for event in self.complete_events]
        write_kernels = '\n'.join(full_events)
        self.remote_machine.write_file(
            os.path.join(self.config.optimization_dir, 'GRADIENT_INFO', 'kernels_list.txt'),
            write_kernels)

        # Copy binaries.
        bins = os.path.join(self.config.specfem_src_dir, 'bin', '*')
        dest = os.path.join(self.config.optimization_dir, 'bin')
        self.remote_machine.execute_command('rsync {} {}'.format(bins, dest))

        # Get DATA directory to fool SPECFEM's optimization routines.
        data_path = os.path.join(alt_solver_dirpath, self.complete_events[0], 'DATA', '*')
        data_dest = os.path.join(self.config.optimization_dir, 'DATA')
        self.remote_machine.execute_command('rsync -a {} {}'.format(data_path, data_dest))

        # Write sbatch.
        with io.open(utilities.get_template_file('sbatch'), 'r') as fh:
            sbatch_string = fh.read().format(**self.sbatch_dict)
        sbatch_path = os.path.join(self.config.optimization_dir, 'run_add_kernels.sbatch')
        with self.remote_machine.ftp_connection.file(sbatch_path, 'wt') as fh:
            fh.write(sbatch_string)

    def check_post_staging(self):
        pass

    def run(self):

        exec_command = 'sbatch run_add_kernels.sbatch'
        _, so, _ = self.remote_machine.execute_command(exec_command,
                                                       workdir=self.config.optimization_dir)

        queue = JobQueue(self.remote_machine, name='Add kernels')
        queue.add_job(utilities.get_job_number_from_stdout(so))
        queue.flash_report(10)

    def check_post_run(self):
        pass
