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

    def make_csv_file_for_noise(self):
        data = obspy.read(os.path.join("./NOISE_CORRELATIONS", "*"))
        network_list = list()
        station_list = list()
        event_list = list()

        for tr in data:
            netDotSta = tr.stats.network + '.' + tr.stats.station + '.' + tr.stats.location
            if netDotSta not in event_list:
                network_list.append(tr.stats.network)
                station_list.append(tr.stats.station)
                event_list.append(netDotSta)

        df = pd.DataFrame()
        df['Network'] = network_list
        df['Station'] = station_list
        df.to_csv('./noise_stations.csv', index=False)
        print "Written all used stations to noise_stations.csv, " \
              "This can be used for downloading the XML files"
        exit()

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


    def __init__(self, remote_machine, config, s_file, get_stations_file):
        super(DownloadStations, self).__init__(remote_machine, config)
        self.client = None
        self.stations_file = s_file
        if get_stations_file == True:
            self.make_csv_file_for_noise()

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
