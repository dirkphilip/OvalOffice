import abc
import os

import subprocess

import click


class System(object):
    """Abstract base representing the cluster which runs the computations.
    """
    __metaclass__ = abc.ABCMeta

    def __init__(self, username=None, firewall=None, cluster=None, python_exec=None):

        self.python_exec = python_exec
        self.username = username
        self.firewall = firewall
        self.cluster = cluster

        self.ssh_connection = None
        self.ftp_connection = None

    @abc.abstractproperty
    def compile_solver(self):
        pass

    @abc.abstractmethod
    def connect(self):
        pass

    @abc.abstractmethod
    def execute_command(self, command, workdir=None, verbose=False):
        """Execute a command line command.

        :param command: The string specifying the command to run.
        :param workdir: Remote working directory in which to execute command.
        :param verbose: Print some information about the command.
        :type command: str
        :type workdir: str
        :raises RuntimeError
        """
        pass

    def makedir(self, remote_dir):
        """Emulates mkdir_p on the remote server.

        :param remote_dir: The remote directory to create.
        :type remote_dir: str
        """

        try:
            self.ftp_connection.chdir(remote_dir)
        except IOError:
            dirname, basename = os.path.split(remote_dir.rstrip('/'))
            self.makedir(dirname)
            self.ftp_connection.mkdir(basename)
            self.ftp_connection.chdir(basename)

    def put_file(self, source_file, dest_file):
        """Put a local file onto the system.

        :param source_file: Full path to source file.
        :param dest_file: Full path to destination file (on System).
        :type source_file: str
        :type dest_file: str
        """
        self.ftp_connection.put(source_file, dest_file)

    def get_file(self, source_file, dest_file):
        """Copy a file from the System to local.

        :param source_file: Full path to source file (on System).
        :param dest_file: Full path to destination file.
        :type source_file: str
        :type dest_file: str
        """
        self.ftp_connection.get(source_file, dest_file)

    def write_file(self, dest_file, contents):
        """Write a text string to a file like object on System.

        :param dest_file: Full path to destination file (on System).
        :param contents: String to write.
        :type dest_file: str
        :type contents: str
        """
        with self.ftp_connection.file(dest_file, 'w') as fh:
            fh.write(contents)

    def read_file(self, source_file):
        """Open a remote file, read it, and return the text as a string.

        :param source_file: Full path to source file (on System).
        :type source_file: str
        :returns: File contents as a string
        """
        with self.ftp_connection.file(source_file, 'r') as fh:
            return fh.readlines()


    def get_rsync(self, remote_op, local_op, verbose=False):
        """Rsync from the machine using rsync remote_op local_op.

        :param remote_op: String specifying rsync command 1 (copy this).
        :param local_op: String specifying rsync command 2 (to this).
        :type remote_op: str
        :type local_op: str
        """

        rsync_string = "rsync {}@{}:{} {}".format(
                                     self.username, self.cluster,
                                     remote_op, local_op)
        if verbose:
            click.secho("Running rsync command: {}".format(rsync_string), fg="yellow")
        sync = subprocess.Popen(rsync_string, shell=True)

        sync.wait()

    def put_rsync(self, local_op, remote_op, verbose=False):
        """Rsync from the machine using rsync local_op remote_op.

        :param local_op: String specifying rsync command 1 (copy this).
        :param remote_op: String specifying rsync command 2 (to this).
        :type remote_op: str
        :type local_op: str
        """

        rsync_string = "rsync {} {}@{}:{}".format(
                                     local_op, self.username, self.cluster,
                                     remote_op)
        if verbose:
            click.secho("Running rsync command: {}".format(rsync_string), fg="yellow")
        sync = subprocess.Popen(rsync_string, shell=True)

        sync.wait()