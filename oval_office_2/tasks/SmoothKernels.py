import io
import os

from oval_office_2 import utilities
from oval_office_2.job_queue import JobQueue
from . import task


class SmoothKernels(task.Task):
    """Submits Smooth Kernel jobs on the remote cluster.

    """
    def __init__(self, remote_machine, config, sbatch_dict, sigma_v, sigma_h):
        super(SmoothKernels, self).__init__(remote_machine, config)
        self.sbatch_dict = sbatch_dict
        self.complete_events = []
        self.event_info, self.iteration_info = utilities.get_lasif_information(
            self.remote_machine, self.config.lasif_project_path,
            self.config.base_iteration)

        self.all_events = sorted(self.event_info.keys())
        self.kernels = None
        self.sigma_v = sigma_v
        self.sigma_h = sigma_h

    def check_pre_staging(self):
        pass

    def stage_data(self):

        kernel_dir = os.path.join(self.config.optimization_dir, 'PROCESSED_KERNELS')
        topo_dir = os.path.join(self.config.solver_dir, 'MESH', 'DATABASES_MPI')
        self.kernels = ['bulk_betah_kernel', 'bulk_betav_kernel', 'bulk_c_kernel', 'eta_kernel', 'hess_inv_kernel']

        # Need to write a specific sbatch script for each kernel.
        for kernel in self.kernels:
            self.sbatch_dict['execute'] = 'srun ./bin/xsmooth_sem {:d} {:d} {} {} {}'.format(
                                                self.sigma_h, self.sigma_v, kernel, kernel_dir, topo_dir)
            self.sbatch_dict['job_name'] = 'smooth_{}'.format(kernel)
            self.sbatch_dict['error'] = 'smooth_{}.stderr'.format(kernel)
            self.sbatch_dict['output'] = 'smooth_{}.stdout'.format(kernel)

            # Write sbatch.
            with io.open(utilities.get_template_file('sbatch'), 'r') as fh:
                sbatch_string = fh.read().format(**self.sbatch_dict)
                sbatch_path = os.path.join(self.config.optimization_dir, 'run_smooth_{}.sbatch'.format(kernel))
            with self.remote_machine.ftp_connection.file(sbatch_path, 'wt') as fh:
                fh.write(sbatch_string)

    def check_post_staging(self):
        pass

    def run(self):
        queue = JobQueue(self.remote_machine, name="Forward Solver")

        for kernel in self.kernels:
                exec_command = "sbatch run_smooth_{}.sbatch".format(kernel)
                _, so, _ = self.remote_machine.execute_command(exec_command,
                                                               workdir=self.config.optimization_dir)
                queue.add_job(utilities.get_job_number_from_stdout(so))

        queue.flash_report(10)

    def check_post_run(self):
        pass
