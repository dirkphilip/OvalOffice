import cPickle
import io
import os

import click


def get_template_file(t_type):
    """Returns the full path for a named template file.

    :param t_type: Name of the template (minus the extension).
    :type t_type: str
    """

    return os.path.join(os.path.dirname(os.path.realpath(__file__)),
                        'templates', '{}.template').format(t_type)


def get_script_file(s_type):
    """Returns the full path for a named python script file.

    :param s_type: Name of the python script (minus the extension).
    :type s_type: str
    """

    if s_type.endswith(".py"):
        s_type.rstrip(".py")

    return os.path.join(os.path.dirname(os.path.realpath(__file__)),
                        "scripts", "{}.py").format(s_type)


def get_lasif_information(system, remote_lasif_dir, iteration):
    """Queries the remote LASIF project, and returns some information.

    :param system: System object describing the remote cluster.
    :param iteration: Relevant LASIF iteration name.
    :param remote_lasif_dir: Directory on system containing lasif project.
    :type remote_lasif_dir: str
    :type iteration: str
    :type system: System
    """

    click.secho("Querying remote LASIF project...", fg="yellow")

    # Copy script
    remote_script = os.path.join(remote_lasif_dir, 'pickle_lasif_project.py')
    system.put_file(get_script_file('pickle_lasif_project'), remote_script)
    #
    # # Run script
    command = '{} {} {}'.format(system.python_exec, remote_script, iteration)
    system.execute_command(command, remote_lasif_dir)

    # Get data
    local_data = './lasif_data.p'
    remote_data = os.path.join(remote_lasif_dir, 'lasif_data.p')
    system.get_file(remote_data, local_data)
    with io.open(local_data, 'rb') as fh:
        data = cPickle.load(fh)

    click.secho("Success!", fg="green")

    # Clean up.
    # os.remove(local_data)
    return data


def set_params_forward_save(sf_dict):
    """Returns a dictionary with the proper parameters set for a forward run
    with the last frame saved.

    :param sf_dict: The basic specfem dict containing things such as nex_xi.
    """

    options = {'model': 'CEM_ACCEPT',
               'simulation_type': 1,
               'save_forward': '.true.',
               'undo_attenuation': '.true.'}
    updated_dict = sf_dict.copy()
    updated_dict.update(options)
    return updated_dict

def set_params_adjoint(sf_dict):
    """Returns a dictionary with the proper parameters set for a forward run
    with the last frame saved.

    :param sf_dict: The basic specfem dict containing things such as nex_xi.
    """

    options = {'model': 'CEM_ACCEPT',
               'simulation_type': 3,
               'save_forward': '.false.',
               'undo_attenuation': '.true.'}
    updated_dict = sf_dict.copy()
    updated_dict.update(options)
    return updated_dict

def set_params_step_length(sf_dict):
    """Returns a dictionary with the proper parameters set for a forward run
    with the last frame saved.

    :param sf_dict: The basic specfem dict containing things such as nex_xi.
    """

    options = {'model': 'CEM_GLL',
               'simulation_type': 1,
               'save_forward': '.false.',
               'undo_attenuation': '.false.'}
    updated_dict = sf_dict.copy()
    updated_dict.update(options)
    return updated_dict

def get_job_number_from_stdout(stdout):
    """Parses the job submission string and returns the job number.

    :param stdout: Paramiko.ChannelFile capturing the stdout.
    :type stdout: Paramiko.ChannelFile
    :return: jobId
    """

    for s in stdout.readlines()[0].split():
        if s.isdigit():
            return int(s)
