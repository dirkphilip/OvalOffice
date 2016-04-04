#!/usr/bin/env python
import inspect

import click

from . import systems
from . import tasks
from .config import Config

pass_config = click.make_pass_decorator(Config, ensure=True)


def _connect_to_system(config):
    """Initializes and connects to the cluster.

    :param config: Config class holding project specific information.
    :type config: Config
    """

    system = systems.system_map['PizDaint'](config.cluster_login,
                                            config.firewall,
                                            config.cluster,
                                            config.python_exec)
    system.connect()
    return system


def _run_task(task):
    """Loops through the job stages in order.

    :param task: Class specifying the operations to be performed.
    :type task: tasks.Task
    """

    for stage in tasks.stages:
        getattr(task, stage)()


@click.group()
@pass_config
def cli(config):
    config.initialize()


@cli.command()
@pass_config
def compile_specfem3d_globe(config):
    """Compiles the solver on the remote machine.
    """

    system = _connect_to_system(config)
    task = tasks.task_map['CompileSpecfem3dGlobe'](system, config)
    _run_task(task)


@cli.command()
@pass_config
def setup_specfem_directories(config):
    """Sets up the directories needed for a specfem run.
    """

    system = _connect_to_system(config)
    task = tasks.task_map['SetupSpecfemDirectories'](system, config)
    _run_task(task)


@cli.command()
@pass_config
def generate_cmt_solutions(config):
    """Generates the CMTSOLUTION file in specfem3d_globe format.

    Reads in the CMT solution template, and populates it with event-specific
    parameters. Then, copies the formatted files to the correct directories
    on the remote machine.
    """

    system = _connect_to_system(config)
    task = tasks.task_map['GenerateCmtSolutions'](system, config)
    _run_task(task)


@cli.command()
@click.option("--regenerate-data-cache", is_flag=True)
@pass_config
def generate_stations_files(config, regenerate_data_cache):
    """Generates the STATIONS file in specfem3d_globe format.

    Generating the proper stations file requires a decent amount of work. This
    is because each station needs to be checked against each event, so that
    only stations that are online for a given event are simulated. Because
    this check is relatively, but not too, expensive, we do it locally. So,
    the LASIF StationXML directory is downloaded and cached.
    """

    system = _connect_to_system(config)
    task = tasks.task_map['GenerateStationsFiles'](system, config, regenerate_data_cache)
    _run_task(task)


@cli.command()
@pass_config
def copy_binaries(config):
    """Copies compiled binaries to relevant scratch directories.

    For each SPECFEM3D_GLOBE run, compiled solver binaries, along with
    information regarding topography, etc., are required in the run directory.
    This function copies all relevant files from the specfem source directory.
    """

    system = _connect_to_system(config)
    task = tasks.task_map['CopyBinariesToRunDirectory'](system, config)
    _run_task(task)


@cli.command()
@pass_config
def copy_raw_data(config):
    """Copies raw data to the remote LASIF project."""

    system = _connect_to_system(config)
    task = tasks.task_map['CopyRawData'](system, config)
    _run_task(task)

@cli.command()
@pass_config
def copy_mseeds(config):
    """Copies mseed files to local directory"""

    system = _connect_to_system(config)
    task = tasks.task_map['CopyMseeds'](system, config)
    _run_task(task)

@cli.command()
@click.option("--nodes", default=1, type=int, help="Total number of nodes.")
@click.option("--ntasks", default=1, type=int, help="Total number of cores.")
@click.option("--time", required=True, type=str, help="Wall time.")
@click.option("--ntasks-per-node", default=1, help="Cores per node.")
@click.option("--cpus-per-task", default=8, help="Threads per core.")
@click.option("--account", default="ch1", help="Account name.")
@click.option("--job-name", default="create_adjoint_sources", help="Name of slurm job.")
@click.option("--output", default="create_adjoint_sources.stdout", help="Capture stdout.")
@click.option("--error", default="create_adjoint_sources.stderr", help="Capture stderr.")
@pass_config
def create_adjoint_sources(config, nodes, ntasks, time, ntasks_per_node, cpus_per_task,
                    account, job_name, output, error):
    """Runs the LASIF provided create_adjoint_sources script on preprocessed and synthetic data."""

    _, _, _, sbatch_dict = inspect.getargvalues(inspect.currentframe())
    sbatch_dict.pop("config")
    sbatch_dict["execute"] = 'aprun -B create_adjoint_sources.py'

    system = _connect_to_system(config)
    task = tasks.task_map['createAdjointSources'](system, config, sbatch_dict)
    _run_task(task)

@cli.command()
@click.option("--nodes", default=1, type=int, help="Total number of nodes.")
@click.option("--ntasks", default=1, type=int, help="Total number of cores.")
@click.option("--time", required=True, type=str, help="Wall time.")
@click.option("--ntasks-per-node", default=1, help="Cores per node.")
@click.option("--cpus-per-task", default=8, help="Threads per core.")
@click.option("--account", default="ch1", help="Account name.")
@click.option("--job-name", default="write_adjoint_sources", help="Name of slurm job.")
@click.option("--output", default="write_adjoint_sources.stdout", help="Capture stdout.")
@click.option("--error", default="write_adjoint_sources.stderr", help="Capture stderr.")
@pass_config
def write_adjoint_sources(config, nodes, ntasks, time, ntasks_per_node, cpus_per_task,
                    account, job_name, output, error):
    """Runs the LASIF provided write_adjoint_sources script on preprocessed and synthetic data."""

    _, _, _, sbatch_dict = inspect.getargvalues(inspect.currentframe())
    sbatch_dict.pop("config")
    sbatch_dict["execute"] = 'aprun -B write_adjoint_sources.py'

    system = _connect_to_system(config)
    task = tasks.task_map['writeAdjointSources'](system, config, sbatch_dict)
    _run_task(task)


@cli.command()
@pass_config
def compare_waveforms(config):
    """Compares synthetic and preprocessed waveforms and shows selected timewindows."""

    system = _connect_to_system(config)
    task = tasks.task_map['CompareWaveforms'](system, config)
    _run_task(task)


@cli.command()
@click.option("--stations_file", type=click.File(),
              help="Formatted file containing station information.",
              required=True)
@pass_config
def download_stations(config, stations_file):
    """Copies raw data to the remote LASIF project."""

    system = _connect_to_system(config)
    task = tasks.task_map['DownloadStations'](system, config, stations_file)
    _run_task(task)


@cli.command()
@click.option("--stations_file", type=click.File(),
              help="Formatted file containing station information.",
              required=True)
@click.option("--recording_time", help="Recoding time (in minutes)",
              default=90)
@pass_config
def download_data(config, stations_file, recording_time):
    """Downloads data from IRIS.

    Given a stations file in the proper format, this script will
    download the appropriate data for a set of Earthquakes queried
    from the LASIF project. By default, the data will be downloaded
    from 5 minutes before the event time, and finish `recording time`
    minutes after the event time.
    """
    system = _connect_to_system(config)
    task = tasks.task_map["DataDownloader"](system, config, stations_file,
                                            recording_time)
    _run_task(task)


@cli.command()
@pass_config
def link_mesh(config):
    """Symlinks the mesh DATABASES_MPI directory to all event directories.

    Each individual event simulation uses the same mesh as is created in the
    MESH subdirectory. This function just places symbolic links in the
    DATABASES_MPI directory of each event simulation directory.
    """

    system = _connect_to_system(config)
    task = tasks.task_map['LinkMesh'](system, config)
    _run_task(task)


@cli.command()
@pass_config
def save_synthetics(config):
    """Saves the consolidated synthetics.mseed files to the LASIF project."""

    system = _connect_to_system(config)
    task = tasks.task_map["SaveSynthetics"](system, config)
    _run_task(task)


@cli.command()
@pass_config
def save_preprocessed_data(config):
    """Saves the consolidated preprocessed_data.mseed files to the LASIF project."""
    system = _connect_to_system(config)
    task = tasks.task_map['SavePreprocessedData'](system, config)
    _run_task(task)



@cli.command()
@click.option("--nodes", default=1, help="Total number of nodes.")
@click.option("--ntasks", default=1, help="Total number of cores.")
@click.option("--time", default="02:00:00", help="Wall time.")
@click.option("--ntasks-per-node", default=1, help="Cores per node.")
@click.option("--cpus-per-task", default=8, help="Threads per core.")
@click.option("--account", default="ch1", help="Account name.")
@click.option("--job-name", default="process_synthetics", help="Name of slurm job.")
@click.option("--output", default="process_synthetics.stdout", help="Capture stdout.")
@click.option("--error", default="process_synthetics.stderr", help="Capture stderr.")
@pass_config
def run_process_synthetics(config, nodes, ntasks, time, ntasks_per_node, cpus_per_task,
                           account, job_name, output, error):
    """Process synthetic data from a recent SPECFEM3D_GLOBE forward solve.

    This command submits the scripts/process_synthetics.py file to run on the
    remote machine.
    """

    _, _, _, sbatch_dict = inspect.getargvalues(inspect.currentframe())
    sbatch_dict.pop("config")
    sbatch_dict["execute"] = "aprun -B process_synthetics.py"

    system = _connect_to_system(config)
    task = tasks.task_map["ProcessSynthetics"](system, config, sbatch_dict)
    _run_task(task)


@cli.command()
@click.option("--nodes", required=True, type=int, help="Total number of nodes.")
@click.option("--ntasks", required=True, type=int, help="Total number of cores.")
@click.option("--time", required=True, type=str, help="Wall time.")
@click.option("--ntasks-per-node", default=1, help="Cores per node.")
@click.option("--cpus-per-task", default=1, help="Threads per core.")
@click.option("--account", default="ch1", help="Account name.")
@click.option("--job-name", default="mesher", help="Name of slurm job.")
@click.option("--output", default="mesher.stdout", help="Capture stdout.")
@click.option("--error", default="mesher.stderr", help="Capture stderr.")
@pass_config
def run_mesher(config, nodes, ntasks, time, ntasks_per_node, cpus_per_task,
               account, job_name, output, error):
    """Writes and submits the sbatch script for running the SPECFEM3D_GLOBE
    internal mesher.
    """

    _, _, _, sbatch_dict = inspect.getargvalues(inspect.currentframe())
    sbatch_dict.pop("config")
    sbatch_dict["execute"] = "aprun -B ./bin/xmeshfem3D"

    system = _connect_to_system(config)
    task = tasks.task_map['RunMesher'](system, config, sbatch_dict)
    _run_task(task)


@cli.command()
@click.option("--nodes", required=True, type=int, help="Total number of nodes.")
@click.option("--ntasks", required=True, type=int, help="Total number of cores.")
@click.option("--time", required=True, type=str, help="Wall time.")
@click.option("--ntasks-per-node", default=1, help="Cores per node.")
@click.option("--cpus-per-task", default=1, help="Threads per core.")
@click.option("--account", default="ch1", help="Account name.")
@click.option("--job-name", default="solver", help="Name of slurm job.")
@click.option("--output", default="solver.stdout", help="Capture stdout.")
@click.option("--error", default="solver.stderr", help="Capture stderr.")
@click.option("--sim-type", default="forward", help="Set type of simulation.")
@pass_config
def run_solver(config, nodes, ntasks, time, ntasks_per_node, cpus_per_task,
               account, job_name, output, error, sim_type):
    """Writes and submits the sbatch script for running the SPECFEM3D_GLOBE
    solver.
    """

    _, _, _, sbatch_dict = inspect.getargvalues(inspect.currentframe())
    sbatch_dict.pop("config")
    sbatch_dict["execute"] = "aprun -B ./bin/xspecfem3D"

    system = _connect_to_system(config)
    task = tasks.task_map["RunSolver"](system, config, sbatch_dict, sim_type)
    _run_task(task)


@cli.command()
@click.option("--nodes", required=True, type=int, help="Total number of nodes.")
@click.option("--ntasks", required=True, type=int, help="Total number of cores.")
@click.option("--time", required=True, type=str, help="Wall time.")
@click.option("--ntasks-per-node", required=True, help="Cores per node.", default=8)
@click.option("--cpus-per-task", default=1, help="Threads per core.")
@click.option("--account", default="ch1", help="Account name.")
@click.option("--job-name", default="select_windows", help="Name of slurm job.")
@click.option("--output", default="select_windows.stdout", help="Capture stdout.")
@click.option("--error", default="select_windows.stderr", help="Capture stderr.")
@pass_config
def run_select_windows(config, nodes, ntasks, time, ntasks_per_node, cpus_per_task,
                       account, job_name, output, error):
    """Run LASIF's window selection algorithm on synthetic data."""

    _, _, _, sbatch_dict = inspect.getargvalues(inspect.currentframe())
    sbatch_dict.pop("config")
    sbatch_dict["execute"] = "./select_windows.py"

    system = _connect_to_system(config)
    task = tasks.task_map["SelectWindows"](system, config, sbatch_dict)
    _run_task(task)


@cli.command()
@click.option("--nodes", default=1, type=int, help="Total number of nodes.")
@click.option("--ntasks", default=1, type=int, help="Total number of cores.")
@click.option("--time", required=True, type=str, help="Wall time.")
@click.option("--ntasks-per-node", default=1, help="Cores per node.")
@click.option("--cpus-per-task", default=8, help="Threads per core.")
@click.option("--account", default="ch1", help="Account name.")
@click.option("--job-name", default="preprocess_data", help="Name of slurm job.")
@click.option("--output", default="preprocess.stdout", help="Capture stdout.")
@click.option("--error", default="preprocess.stderr", help="Capture stderr.")
@pass_config
def preprocess_data(config, nodes, ntasks, time, ntasks_per_node, cpus_per_task,
                    account, job_name, output, error):
    """Runs the LASIF provided preprocessing scripts on raw data."""

    _, _, _, sbatch_dict = inspect.getargvalues(inspect.currentframe())
    sbatch_dict.pop("config")
    sbatch_dict["execute"] = 'aprun -B preprocess_data.py'

    system = _connect_to_system(config)
    task = tasks.task_map['PreprocessData'](system, config, sbatch_dict)
    _run_task(task)


@cli.command()
@click.option("--nodes", default=3, type=int, help="Total number of nodes.")
@click.option("--ntasks", default=24, type=int, help="Total number of cores.")
@click.option("--time", default='00:10:00', type=str, help="Wall time.")
@click.option("--ntasks-per-node", default=8, help="Cores per node.")
@click.option("--cpus-per-task", default=1, help="Threads per core.")
@click.option("--account", default="ch1", help="Account name.")
@click.option("--job-name", default="sum_kernels", help="Name of slurm job.")
@click.option("--output", default="sum_kernels.stdout", help="Capture stdout.")
@click.option("--error", default="sum_kernels.stderr", help="Capture stderr.")
@pass_config
def sum_kernels(config, nodes, ntasks, time, ntasks_per_node, cpus_per_task,
                account, job_name, output, error):
    _, _, _, sbatch_dict = inspect.getargvalues(inspect.currentframe())
    sbatch_dict.pop('config')
    sbatch_dict['execute'] = 'aprun -B ./bin/xsum_preconditioned_kernels'

    system = _connect_to_system(config)
    task = tasks.task_map['SumGradients'](system, config, sbatch_dict)
    _run_task(task)


if __name__ == "__main__":
    cli()
