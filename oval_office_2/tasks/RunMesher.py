import io
import os

from oval_office_2 import utilities
from . import task


class RunMesher(task.Task):
    """Writes and submits the sbatch script for running the SPECFEM3D_GLOBE
    internal mesher.
    """

    def __init__(self, remote_machine, config, sbatch_dict):
        super(RunMesher, self).__init__(remote_machine, config)
        self.sbatch_dict = sbatch_dict

    def check_pre_staging(self):
        pass

    def stage_data(self):
        with io.open(utilities.get_template_file("sbatch"), "r") as fh:
            sbatch_string = fh.read().format(**self.sbatch_dict)

        sbatch_path = os.path.join(self.config.solver_dir, "MESH", "run_mesher.sbatch")
        with self.remote_machine.ftp_connection.file(sbatch_path, "wt") as fh:
            fh.write(sbatch_string)

    def check_post_staging(self):
        pass

    def run(self):
        mesh_dir = os.path.join(self.config.solver_dir, "MESH")
        exec_command = "sbatch run_mesher.sbatch"
        _, so, _ = self.remote_machine.execute_command(exec_command,
                                                       workdir=mesh_dir)

        job_id = utilities.get_job_number_from_stdout(so)

    def check_post_run(self):
        pass
