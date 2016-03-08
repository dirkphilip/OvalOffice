import click
import paramiko
from . import system


class PizDaint(system.System):
    def __del__(self):

        if self.ssh_connection:
            click.secho("Closing ssh connection to daint.cscs.ch..", fg="yellow")
            self.ssh_connection.close()
            click.secho("Success!", fg="green")

    @property
    def compile_solver(self):
        return """
            cd {dir};
            module purge;
            make clean;
            module load PrgEnv-gnu;
            module load adios/1.8.0_gnu;
            module load cray-mpich;
            module load cray-netcdf-hdf5parallel/4.3.1;
            module load cudatoolkit;

            FC=ftn \
            CC=cc \
            MPIF90=ftn \
            MPICC=cc \
            CEM_FCFLAGS="-I$NETCDF_DIR/include" \
            CEM_LIBS="-L$NETCDF_DIR -lnetcdf_parallel" \
            CEM_FCFLAGS="-I$NETCDF_DIR/include" \
            CUDA_INC=$CRAY_CUDATOOLKIT_DIR/include \
            CUDA_LIB=$CRAY_CUDATOOLKIT_DIR/lib64 MPI_INC=$CRAY_MPICH2_DIR/include \
            FLAGS_CHECK='-O3' \
            CFLAGS='-O3' \
            FCFLAGS='-O3' \
            ADIOS_INC=$ADIOS_INC \
            ADIOS_LIB=$ADIOS_FLIB \
            ./configure \
            --with-cuda=cuda5 \
            --with-adios \
            --with-cem

            mkdir -p bin

            CRAY_CPU_TARGET=x86_64 make -j 4
        """

    def connect(self):

        click.secho("Attempting to connect to daint.cscs.ch...", fg="yellow")
        try:
            self.ssh_connection = paramiko.SSHClient()
            self.ssh_connection.load_system_host_keys()
            self.ssh_connection.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            proxy = paramiko.ProxyCommand(
                "ssh -q -Y %s@%s netcat %s 22 -w 10" % (self.username, self.firewall, self.cluster))
            # noinspection PyTypeChecker
            self.ssh_connection.connect("daint.cscs.ch", username=self.username, sock=proxy)
            self.ftp_connection = self.ssh_connection.open_sftp()
        except:
            raise RuntimeError("Failed to connect to daint.cscs.ch.")
        click.secho("Success!", fg="green")

    def execute_command(self, command, workdir=None, verbose=False):

        if verbose:
            click.secho("Executing command: {}".format(command), color='yellow')

        # Execute command (stdin, stdout, stderr).
        if workdir:
            si, so, se = self.ssh_connection.exec_command(
                "cd {}; {}".format(workdir, command))
        else:
            si, so, se = self.ssh_connection.exec_command(
                "{}".format(command))

        if so.channel.recv_exit_status():
            raise RuntimeError("Command: {} has returned a "
                               "non-zero exit code:\n {}".format(command, se.readlines()))

        return si, so, se
