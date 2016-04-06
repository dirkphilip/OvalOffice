import io
import os

import click

from . import task
from .. import utilities
from ..job_queue import JobQueue


class RunSolver(task.Task):
    """Submits Specfem jobs on the remote cluster.

    """

    def __init__(self, remote_machine, config, sbatch_dict, sim_type, specific_events=None):
        super(RunSolver, self).__init__(remote_machine, config)
        self.event_info, self.iteration_info = utilities.get_lasif_information(
            self.remote_machine, self.config.lasif_project_path,
            self.config.base_iteration)

        if specific_events:
            for e in specific_events:
                if e not in self.event_info.keys():
                    raise RuntimeError(
                        "Specific event {} not in project".format(e))
            for key in self.event_info.keys():
                if key not in specific_events:
                    self.event_info.pop(key)

        self.all_events = sorted(self.event_info.keys())
        self.sbatch_dict = sbatch_dict
        self.sim_type = sim_type
        self.failed_jobs = None

    def check_pre_staging(self):
        pass

    def stage_data(self):
        print self.sim_type
        if self.sim_type == 'adjoint':
            with io.open(utilities.get_template_file("Par_file"), "r") as fh:
                par_file_string = fh.read().format(
                    **utilities.set_params_adjoint(self.config.specfem_dict))

        else:
            with io.open(utilities.get_template_file("Par_file"), "r") as fh:
                par_file_string = fh.read().format(
                    **utilities.set_params_forward_save(self.config.specfem_dict))

        with click.progressbar(self.all_events, label="Writing files...") as events:
            for event in events:

                self.sbatch_dict["job_name"] = event
                with io.open(utilities.get_template_file("sbatch"), "r") as fh:
                    sbatch_string = fh.read().format(**self.sbatch_dict)

                sbatch_path = os.path.join(self.config.solver_dir, event, "run_solver.sbatch")
                par_file_path = os.path.join(self.config.solver_dir, event, "DATA", "Par_file")
                self.remote_machine.write_file(sbatch_path, sbatch_string)
                self.remote_machine.write_file(par_file_path, par_file_string)

    def check_post_staging(self):
        with click.progressbar(self.all_events, label="Validating mesh files...") as events:
            for event in events:
                mesh_dir = os.path.join(self.config.solver_dir, event, "DATABASES_MPI")
                output_dir = os.path.join(self.config.solver_dir, event, "OUTPUT_FILES")
                files = self.remote_machine.ftp_connection.listdir(mesh_dir)
                output_files = self.remote_machine.ftp_connection.listdir(output_dir)
                num_reg1 = sum("reg1_solver_data" in f for f in files)
                num_reg2 = sum("reg2_solver_data" in f for f in files)
                num_reg3 = sum("reg3_solver_data" in f for f in files)
                required_output = ["addressing.txt", "output_mesher.txt",
                                   "values_from_mesher.h"]
                try:
                    assert num_reg1 == num_reg2 == num_reg3
                    for r in required_output:
                        assert r in output_files
                except:
                    raise RuntimeError("Some issue with mesh file linkage. "
                                      "Please check mesher.")

    def submitJobs(self,all_events):
        queue = JobQueue(self.remote_machine, name="Forward Solver")
        exec_command = "sbatch run_solver.sbatch"
        with click.progressbar(all_events, label="Submitting jobs...") as events:
            for event in events:
                event_dir = os.path.join(self.config.solver_dir, event)
                _, so, _ = self.remote_machine.execute_command(exec_command,
                                                               workdir=event_dir)
                queue.add_job(utilities.get_job_number_from_stdout(so))

        queue.flash_report(10)

    def run(self):
        self.submitJobs(self.all_events)

    def check_post_run(self):
        self.failed_jobs = []
        with click.progressbar(self.all_events, label="Checking results...") as events:
            for event in events:

                event_dir = os.path.join(self.config.solver_dir, event)
                output_dir = os.path.join(event_dir, "OUTPUT_FILES")
                stations_file = os.path.join(event_dir, "DATA", "STATIONS")
                stations = self.remote_machine.read_file(stations_file)
                outputs = self.remote_machine.ftp_connection.listdir(output_dir)
                trgts = {".".join([x.split()[1], x.split()[0]]) for x in stations}
                avail = {".".join([x.split(".")[0], x.split(".")[1]]) for x in outputs if x.endswith(".sac")}
                if not trgts == avail:
                    self.failed_jobs.append(event)

        if not self.failed_jobs:
            click.secho("All events seem to have completed normally.", fg="green")
        else:
            click.secho("FAILED EVENTS", fg="red")
            click.echo("\n".join(self.failed_jobs))
            self.submitJobs(self.failed_jobs)