import cPickle
import io
import os

from oval_office_2 import utilities
from . import task
from ..job_queue import JobQueue

class ProcessSynthetics(task.Task):

    def __init__(self, system, config, sbatch_dict):
        super(ProcessSynthetics, self).__init__(system, config)
        self.event_info, self.iteration_info = utilities.get_lasif_information(
            self.remote_machine, self.config.lasif_project_path,
            self.config.base_iteration)
        self.sbatch_dict = sbatch_dict

    def check_pre_staging(self):
        pass

    def stage_data(self):

        # Write script
        remote_script = os.path.join(self.config.solver_dir, "process_synthetics.py")
        with io.open(utilities.get_script_file("process_synthetics"), "r") as fh:
            script_string = fh.readlines()
        script_string.insert(0, "#!{}\n".format(self.config.python_exec))
        self.remote_machine.write_file(remote_script, "".join(script_string))

        # Copy over pickle file.
        info = {"lowpass": self.iteration_info["lowpass"],
                "highpass": self.iteration_info["highpass"],
                "event_list": self.event_info.keys()}
        tmp_pickle = "tmp_pickle.p"
        remote_pickle = os.path.join(self.config.solver_dir, "info.p")
        with io.open(tmp_pickle, "wb") as fh:
            cPickle.dump(info, fh)
        self.remote_machine.put_file(tmp_pickle, remote_pickle)
        os.remove(tmp_pickle)

        # Copy sbatch file.
        remote_sbatch = os.path.join(self.config.solver_dir, "process_synthetics.sbatch")
        with io.open(utilities.get_template_file("sbatch"), "r") as fh:
            sbatch_string = fh.read().format(**self.sbatch_dict)
        self.remote_machine.write_file(remote_sbatch, sbatch_string)

    def check_post_staging(self):
        pass

    def run(self):
        pass

        # exec_command = "chmod +x process_synthetics.py; sbatch process_synthetics.sbatch"
        # queue = JobQueue(self.remote_machine, name="Process Synthetics")
        # _, so, _ = self.remote_machine.execute_command(
        #     exec_command, workdir=self.config.solver_dir)
        # queue.add_job(utilities.get_job_number_from_stdout(so))
        #
        # queue.flash_report(10)

    def check_post_run(self):
        pass

