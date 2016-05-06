import io
import os

from oval_office_2 import utilities
from oval_office_2.job_queue import JobQueue

from . import task


class RunMesher(task.Task):
    """Writes and submits the sbatch script for running the SPECFEM3D_GLOBE
    internal mesher.
    """

    def __init__(self, remote_machine, config, sbatch_dict,model_type):
        super(RunMesher, self).__init__(remote_machine, config)
        self.sbatch_dict = sbatch_dict
        #self.model_type = model_type
        self.model_type = '1D_isotropic_prem'

    def check_pre_staging(self):
        pass

    def stage_data(self):

        with io.open(utilities.get_template_file("sbatch"), "r") as fh:
            sbatch_string = fh.read().format(**self.sbatch_dict)

        sbatch_path = os.path.join(self.config.solver_dir, "MESH", "run_mesher.sbatch")
        with self.remote_machine.ftp_connection.file(sbatch_path, "wt") as fh:
            fh.write(sbatch_string)

        par_file_path = os.path.join(
            self.config.solver_dir, "MESH", "DATA", "Par_file")

        if self.model_type == 'CEM_GLL':
            print 'using model type: step_length'
            if self.config.simulation_type == 'regional':
                with io.open(utilities.get_template_file("Par_file_regional"), "rt") as fh:
                    self.remote_machine.write_file(
                        par_file_path,
                        fh.read().format(**utilities.set_params_step_length(self.config.specfem_dict)))
            else:
                with io.open(utilities.get_template_file("Par_file"), "rt") as fh:
                    self.remote_machine.write_file(
                        par_file_path,
                        fh.read().format(**utilities.set_params_step_length(self.config.specfem_dict)))

        elif self.model_type == 'CEM':
            if self.config.simulation_type == 'regional':
                with io.open(utilities.get_template_file("Par_file_regional"), "rt") as fh:
                    self.remote_machine.write_file(
                        par_file_path,
                        fh.read().format(**utilities.set_params_forward_save(self.config.specfem_dict)))
            elif self.config.simulation_type == 'global':
                with io.open(utilities.get_template_file("Par_file"), "rt") as fh:
                    self.remote_machine.write_file(
                        par_file_path,
                        fh.read().format(**utilities.set_params_forward_save(self.config.specfem_dict)))

    def check_post_staging(self):
        pass

    def run(self):

        mesh_dir = os.path.join(self.config.solver_dir, "MESH")
        exec_command = "sbatch run_mesher.sbatch"
        _, so, _ = self.remote_machine.execute_command(exec_command,
                                                       workdir=mesh_dir)

        queue = JobQueue(self.remote_machine, name="Mesher")
        queue.add_job(utilities.get_job_number_from_stdout(so))
        queue.flash_report(10)

    def check_post_run(self):
        pass
