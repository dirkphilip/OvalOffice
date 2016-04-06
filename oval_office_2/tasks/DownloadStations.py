from collections import namedtuple
from itertools import repeat

import click
import obspy
import os
import pandas as pd

import boltons.fileutils
from concurrent import futures
from obspy.clients.fdsn import Client
from obspy.clients.fdsn.header import FDSNException
from oval_office_2 import utilities
from . import task

Station = namedtuple('Station', 'net sta')
MAX_WORKERS = 5


class DownloadStations(task.Task):
    """Downloads data from IRIS.

    Given a stations file in the proper format, this script will
    download the appropriate data for a set of Earthquakes queried
    from the LASIF project.
    """
    def download(self, s):

        client = Client("IRIS")
        starttime = obspy.UTCDateTime("2010-01-01")
        endtime = obspy.UTCDateTime("2015-01-02")

        # for s in stations:
            # Try and download, don't worry if no data is available.
        try:
            stream = client.get_stations(network=s.net, station=s.sta, starttime=starttime, endtime=endtime,
                                         level="channel")
            fname = os.path.join("STATION_XML_META", "station.{}_{}.meta.xml".format(s.net, s.sta))
            stream.write(fname, format='STATIONXML')
        except Exception as e:
            print e
            pass

        try:
            stream = client.get_stations(network=s.net, station=s.sta, starttime=starttime, endtime=endtime,
                                         level="response")
            fname = os.path.join("STATION_XML_META", "station.{}_{}.response.xml".format(s.net, s.sta))
            stream.write(fname, format='STATIONXML')
            print 'Finished downloading {}.{}'.format(s.net, s.sta)
        except Exception as e:
            print e
            pass


    def __init__(self, remote_machine, config, s_file):
        super(DownloadStations, self).__init__(remote_machine, config)
        self.event_info, self.iteration_info = utilities.get_lasif_information(
            self.remote_machine, self.config.lasif_project_path,
            self.config.base_iteration)
        self.client = None
        self.stations_file = s_file

    def check_pre_staging(self):
        pass

    def stage_data(self):

        self.sf = pd.read_csv(self.stations_file)

        # Make a local directory for data.
        boltons.fileutils.mkdir_p("STATION_XML_META")

    def check_post_staging(self):
        pass

    def run(self):

        stations = [Station(sta=row.loc["Station"], net=row.loc["Network"])
                    for _, row in self.sf.iterrows()]

        #multithreaded version
        with futures.ThreadPoolExecutor(MAX_WORKERS) as executor:
           executor.map(self.download, stations)

        #single threaded version
        #self.download(stations)

    def check_post_run(self):
        xml_dir = os.path.join("STATION_XML_META", "station.*.xml")
        remote_station = os.path.join(self.config.lasif_project_path, "STATIONS", "StationXML")
        self.remote_machine.put_rsync(xml_dir, remote_station, verbose=True)
