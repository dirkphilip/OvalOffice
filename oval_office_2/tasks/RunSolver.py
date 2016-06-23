import io
import os

import boltons.fileutils
import click
import cPickle
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
        self.maxRetries = 3
        self.only_failed = False

    def check_pre_staging(self):
        pass

    def stage_data(self):
        print self.sim_type
        if self.sim_type == 'forward_step_length':
            # Get misfit file
            misfit_file_path = os.path.join(self.config.lasif_project_path,
                                            "ADJOINT_SOURCES_AND_WINDOWS/ADJOINT_SOURCES",
                                            self.config.first_iteration, 'misfit.p')
            self.remote_machine.get_file(misfit_file_path, "./misfit.p")

            with open("./misfit.p", 'rb') as fh:
                misfit_iter1 = cPickle.load(fh)

            # Create format with total misfit per event
            dict_iter1 = {}
            for misfit in misfit_iter1[:]:
                event = misfit[0]
                station_misfits = misfit[1]
                dict_iter1[misfit[0]] = sum(station_misfits.values())

            # Sort and select 25 with highest misfits
            events_sorted_by_misfit = sorted(dict_iter1.items(), key=lambda x:x[1], reverse=True)
            self.all_events = [x[0] for x in events_sorted_by_misfit[:25]]

        if self.config.simulation_type == 'regional':
            with io.open(utilities.get_template_file("Par_file_regional"), "r") as fh:
                par_file_string = fh.read().format(
                    **utilities.set_params_step_length(self.config.specfem_dict))
        elif self.config.simulation_type == 'global':
            with io.open(utilities.get_template_file("Par_file"), "r") as fh:
                par_file_string = fh.read().format(
                    **utilities.set_params_step_length(self.config.specfem_dict))

        if self.sim_type == 'adjoint':
            if self.config.simulation_type == 'regional':
                with io.open(utilities.get_template_file("Par_file_regional"), "r") as fh:
                    par_file_string = fh.read().format(
                        **utilities.set_params_adjoint(self.config.specfem_dict,
                                                       self.config.model))
            elif self.config.simulation_type == 'global':
                with io.open(utilities.get_template_file("Par_file"), "r") as fh:
                    par_file_string = fh.read().format(
                        **utilities.set_params_adjoint(self.config.specfem_dict,
                                                       self.config.model))

        elif self.sim_type == 'forward':
            if self.config.simulation_type == 'regional':
                with io.open(utilities.get_template_file("Par_file_regional"), "r") as fh:
                    par_file_string = fh.read().format(
                        **utilities.set_params_forward_save(self.config.specfem_dict,
                                                            self.config.model))

            elif self.config.simulation_type == 'global':
                with io.open(utilities.get_template_file("Par_file"), "r") as fh:
                    par_file_string = fh.read().format(
                        **utilities.set_params_forward_save(self.config.specfem_dict,
                                                            self.config.model))

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
        if self.sim_type == 'forward_step_length':
            all_events = all_events[:25]
        with click.progressbar(all_events, label="Submitting jobs...") as events:
            for event in events:
                event_dir = os.path.join(self.config.solver_dir, event)
                _, so, _ = self.remote_machine.execute_command(exec_command,
                                                               workdir=event_dir)
                queue.add_job(utilities.get_job_number_from_stdout(so))

        queue.flash_report(10)

    def run(self):
        if self.only_failed:
            self.check_jobs()
            self.submitJobs(self.failed_jobs)
        else:
            self.submitJobs(self.all_events)

    def check_post_run(self):
        self.check_jobs()
        count = 0
        while len(self.failed_jobs) > 0 and count < self.maxRetries:
            self.submitJobs(self.failed_jobs)
            count += 1
            self.check_jobs()
        # Write failed jobs to a .txt file
        output_dir = os.path.join("OUTPUT", self.config.base_iteration)
        boltons.fileutils.mkdir_p(output_dir)
        os.path.join(output_dir,self.sim_type)
        text_file = open(os.path.join(output_dir,self.sim_type), "w")
        text_file.write("\n".join(self.failed_jobs))
        text_file.close()

    def check_jobs(self):
        self.failed_jobs = []
        with click.progressbar(self.all_events, label="Checking results...") as events:
            for event in events:
                event_dir = os.path.join(self.config.solver_dir, event)
                output_dir = os.path.join(event_dir, "OUTPUT_FILES")
                outputs = self.remote_machine.ftp_connection.listdir(output_dir)
                if self.sim_type == 'adjoint':
                    npts = str(self.iteration_info['npts']).zfill(6)
                    out_file = 'timestamp_backward_and_adjoint' + npts
                    if out_file not in outputs:
                        self.failed_jobs.append(event)
                else:
                    stations_file = os.path.join(event_dir, "DATA", "STATIONS")
                    stations = self.remote_machine.read_file(stations_file)
                    trgts = {".".join([x.split()[1], x.split()[0]]) for x in stations}
                    avail = {".".join([x.split(".")[0], x.split(".")[1]]) for x in outputs if x.endswith(".sac")}
                    if not trgts == avail:
                        self.failed_jobs.append(event)
        if not self.failed_jobs:
            click.secho("All events seem to have completed normally.", fg="green")
        else:
            click.secho("FAILED EVENTS", fg="red")
            click.echo("\n".join(self.failed_jobs))
