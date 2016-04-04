import io
import os

from . import task
from .. import utilities


class CompileSpecfem3dGlobe(task.Task):
    """Compile specfem3D_globe on the remote_machine."""

    def check_pre_staging(self):
        pass

    def stage_data(self):

        with io.open(utilities.get_template_file("Par_file"), "rt") as fh:
            r_file = self.remote_machine.ftp_connection.file(
                os.path.join(self.config.specfem_src_dir, 'DATA', 'Par_file'), "w")
            r_file.write(fh.read().format(
                **utilities.set_params_forward_save(self.config.specfem_dict)))

    def check_post_staging(self):
        pass

    def run(self):
        # Compile solver.
        self.remote_machine.execute_command(
            self.remote_machine.compile_solver.format(
                dir=self.config.specfem_src_dir))

        # Compile smoother.
        self.remote_machine.execute_command(
            self.remote_machine.compile_tomo.format(
                dir=self.config.specfem_src_dir))

    def check_post_run(self):
        pass
