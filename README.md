[![Build Status](https://travis-ci.org/michael-afanasiev/OvalOffice.svg?branch=master)](https://travis-ci.org/michael-afanasiev/OvalOffice)


Explanation of parameters which are set in the config.json file (made by running an oo_2 command).

base_iteration: This is the name of the first, which has to be made in the remote LASIF directory.

cluster: The address of the cluster, e.g. "daint.cscs.ch".

cluster_login" log in name used for logging in onto the cluster. Make sure password free login is setup.

firewall: The address of the firewall, e.g. "ela.cscs.ch".

first_iteration: This is the name of the first iteration, which is used to get the selected windows from. 

iteration_to_update: This is currently not used in oo_2, but could be useful to use.

lasif_project_path: path to of the remote LASIF directory, e.g. "/project/ch1/username/LASIF_africa".

nex_eta: This is a SPECFEM parameter, refer to the specfem manual.

nex_xi: This is a SPECFEM parameter, refer to the specfem manual.

nproc_eta: This is a SPECFEM parameter, refer to the specfem manual.

nproc_xi: This is a SPECFEM parameter, refer to the specfem manual.

project_name: This will be the name of the project directory on scratch.

python_exec: Location of Python executable, e.g. "/users/dpvanher/anaconda2/bin/python"

scratch_path: user's scratch location, e.g. "/scratch/daint/dpvanher/"

simulation_time: Simulation time in minutes, e.g. "45"

specfem_src_dir: Location of the SPECFEM directory "/users/dpvanher/specfem3d_globe",

input_data_type: type of data used for modelling. Set to "noise" for noise cross-correlations, set to "earthquake" when using earthquake data.

simulation_type: Type of simulation to be performed by SPECFEM "regional" or "global"

For further settings check the templates in the oval_office_2/templates directory

