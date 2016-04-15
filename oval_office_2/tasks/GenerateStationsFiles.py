import cPickle
import io
import os

import boltons.fileutils
import click
import obspy

from oval_office_2 import utilities
from . import task


class GenerateStationsFiles(task.Task):
    """Generates the STATIONS file in specfem3d_globe format.

    Generating the proper stations file requires a decent amount of work. This
    is because each station needs to be checked against each event, so that
    only stations that are online for a given event are simulated. Because
    this check is relatively, but not too, expensive, we do it locally. So,
    the LASIF StationXML directory is downloaded and cached.
    """

    def __init__(self, remote_machine, config, regenerate_data_cache):
        super(GenerateStationsFiles, self).__init__(remote_machine, config)
        self.event_info, self.iteration_info = utilities.get_lasif_information(
            self.remote_machine, self.config.lasif_project_path,
            self.config.base_iteration)
        self.regenerate_data_cache = regenerate_data_cache

    def check_pre_staging(self):
        pass

    def stage_data(self):

        # Get data summary
        remote_script = os.path.join(self.config.lasif_project_path,
                                     'pickle_lasif_data.py')
        self.remote_machine.put_file(
            utilities.get_script_file("pickle_lasif_data"), remote_script)

        # Run script
        if self.regenerate_data_cache:
            click.secho("Generating data cache...")
            command = '{} {} {}'.format(self.remote_machine.python_exec,
                                        remote_script,
                                        self.config.base_iteration)
            self.remote_machine.execute_command(
                command, self.config.lasif_project_path)

        # Read data cache.
        data_cache = os.path.join(self.config.lasif_project_path,
                                  "data_pickle.p")
        self.remote_machine.get_file(data_cache, "./data_pickle.p")

    def check_post_staging(self):
        pass

    def run(self):

        with io.open("./data_pickle.p", "rb") as fh:
            required_stations = cPickle.load(fh)

        # Get stations xml.
        #local_stations = "./STATION_XML_META/"
        # local_stations = "./NoiseXML/"
        # lasif_stations = os.path.join(self.config.lasif_project_path,
        #                               "STATIONS", "StationXML", "*meta*")
        # boltons.fileutils.mkdir_p(local_stations)
        # self.remote_machine.get_rsync(lasif_stations, local_stations)
        with click.progressbar(sorted(self.event_info.keys()) + ["MESH"],
                               label="Writing stations files...") as events:
            for event in events:
                print required_stations
                write_stations = []
                for information in required_stations:

                    # Just take some event for the mesh directory.
                    if event == "MESH":
                        start_time = self.event_info[
                            self.event_info.keys()[0]]['origin_time']
                    else:
                        start_time = self.event_info[event]["origin_time"]
                    # Get station properties at time of event.
                    net, sta, loc = information
                    # station_xml_name = os.path.join(
                    #     "./STATION_XML_META",
                    #     "station.{}_{}.meta.xml".format(net, sta))
                    station_xml_name = os.path.join(
                        "./NoiseXML",
                        "{}.{}.xml".format(net, sta))

                    try:
                        inv = obspy.read_inventory(station_xml_name,
                                                   format="stationxml")

                        contents = inv.get_contents()['channels']
                        station_dict = inv.get_coordinates(contents)

                        station_dict["net"] = net
                        station_dict["sta"] = sta
                        write_stations.append(station_dict)
                    except Exception as e:
                        print e
                        # If station is not found at event time, leave it out.
                        pass
                # Generate strings for unique stations.
                unique_stations = set()
                for s in write_stations:
                    station_file_string = '%-6s %-6s %-8.3f %-8.3f %-8.1f %-8.1f' \
                                           % (s['sta'], s['net'], s['latitude'], s['longitude'], s['elevation'],
                                              s['local_depth'])
                    unique_stations.add(station_file_string)

                # Save to remote.
                station_file_write = "\n".join(unique_stations)
                stations_file = os.path.join(self.config.solver_dir, event, "DATA", "STATIONS")
                with self.remote_machine.ftp_connection.file(stations_file, "w") as fh:
                    fh.write(station_file_write)

    def check_post_run(self):
        pass
