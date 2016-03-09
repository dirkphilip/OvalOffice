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
@click.option("--output", default="mesher.stdout", help="Capture stdout.")
@click.option("--error", default="mesher.stderr", help="Capture stderr.")
@pass_config
def run_solver(config, nodes, ntasks, time, ntasks_per_node, cpus_per_task,
               account, job_name, output, error):
    """Writes and submits the sbatch script for running the SPECFEM3D_GLOBE
    solver.
    """

    specific_event = ["GCMT_event_ALASKA_PENINSULA_Mag_5.7_2011-11-6-8",
                      "GCMT_event_ANDAMAN_ISLANDS,_INDIA_REGION_Mag_5.6_2013-11-20-10"]
    _, _, _, sbatch_dict = inspect.getargvalues(inspect.currentframe())
    sbatch_dict.pop("config")
    sbatch_dict["execute"] = "aprun -B ./bin/xspecfem3D"

    system = _connect_to_system(config)
    task = tasks.task_map["RunSolver"](system, config, sbatch_dict, specific_events=specific_event)
    _run_task(task)


if __name__ == "__main__":
    cli()
