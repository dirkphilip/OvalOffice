
import io
import os
import json
import click

DEFAULT_CONFIG = {
    "project_name": "x",
    "lasif_project_path": "x",
    "specfem_src_dir": "x",
    "scratch_path": "x",
    "firewall": "x",
    "cluster": "daint",
    "cluster_login": "x",
    "nex_xi": "x",
    "nex_eta": "x",
    "nproc_xi": "x",
    "nproc_eta": "x",
    "python_exec": "x",
    "base_iteration": "x",
    "simulation_time": "x"
}

CONFIG_FILE = os.path.join('./config.json')

class Config(object):
    """Contains basic parameters for the job (paths, etc.)"""

    def __init__(self):

        self.project_name = None
        self.lasif_project_path = None
        self.specfem_src_dir = None
        self.scratch_path = None
        self.firewall = None
        self.cluster = None
        self.cluster_login = None
        self.nex_xi = None
        self.nex_eta = None
        self.nproc_xi = None
        self.nproc_eta = None
        self.python_exec = None
        self.iteration_name = None
        self.specfem_dict = None
        self.base_iteration = None
        self.simulation_time = None

    def initialize(self):
        """Populates the class from ./config.json.

        If ./config.json does not exist, writes a default file and exits.
        """

        if not os.path.exists(CONFIG_FILE):
            with io.open(CONFIG_FILE, 'wb') as fh:
                json.dump(DEFAULT_CONFIG, fh, sort_keys=True, indent=4, separators=(",", ": "))
            click.secho("Could not find configuration file. I've written an empty config.json file "
                        "in the current directory. Edit this and re-run.", color='red')
            exit()

        # Load all options.
        with io.open(CONFIG_FILE, 'r') as fh:
            data = json.load(fh)
        for key, value in data.iteritems():
            setattr(self, key, value)

        # Create specific dictionary for specfem options.
        self.specfem_dict = {'NEX_XI': int (self.nex_xi),
                             'NEX_ETA': int (self.nex_eta),
                             'NPROC_XI': int (self.nproc_xi),
                             'NPROC_ETA': int (self.nproc_eta),
                             'simulation_time': int (self.simulation_time)}

    @property
    def work_dir(self):
        """Scratch directory under which all work is done."""
        return os.path.join(self.scratch_path, self.project_name,
                            self.base_iteration)

    @property
    def solver_dir(self):
        """Directory within which the solver is run (for all events)."""
        return os.path.join(self.work_dir, "SOLVER")

    @property
    def window_dir(self):
        """Directory within which window-picking programs are run."""
        return os.path.join(self.work_dir, "WINDOW_PICKING")

    @property
    def adjoint_dir(self):
        """Directory within whcih adjoint sources are calculated."""
        return os.path.join(self.work_dir, 'ADJOINT_SOURCES')

    @property
    def preprocessing_dir(self):
        return os.path.join(self.work_dir, 'DATA_PREPROCESSING')

