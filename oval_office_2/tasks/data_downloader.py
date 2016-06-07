import os
from collections import namedtuple
from itertools import repeat

import boltons.fileutils
import click
import obspy
import pandas as pd
from concurrent import futures
from obspy.clients.fdsn import Client
from obspy.clients.fdsn.header import FDSNException

from oval_office_2 import utilities
from . import task

Event = namedtuple('Event', 'name start end')
Station = namedtuple('Station', 'net sta')
MAX_WORKERS = 10


class DataDownloader(task.Task):
    """Downloads data from IRIS.

    Given a stations file in the proper format, this script will
    download the appropriate data for a set of Earthquakes queried
    from the LASIF project.
    """

    @staticmethod
    def _download(stations, event):

        click.secho("Downloading: {}".format(event.name))
        stream = obspy.Stream()
        client = Client("IRIS")
        for s in stations:
            req = (s.net, s.sta, '*', 'BH*', event.start, event.end)

            # Try and download, don't worry if no data is available.
            try:
                stream += client.get_waveforms(*req)
                print "Retrieved {}.{}".format(s.net, s.sta)
            except FDSNException as e:
                print "Could not retrieve {}.{}".format(s.net, s.sta)
                pass

        fname = os.path.join("RAW_DATA", "{}.mseed".format(event.name))
        stream.write(fname, format='mseed')

    @staticmethod
    def _download_bulk(stations, event):

        click.secho("Downloading: {}".format(event.name))
        client = Client("IRIS")

        # Remove duplicate stations. For some reasons NARS breaks obspy.
        bulk_req = [(s.net, s.sta, '*', 'BHE,BHN,BHZ') for s in stations]
        bulk_req = [s + (event.start, event.end) for s in set(bulk_req) if "NARS" not in s]

        filename = os.path.join("RAW_DATA", "{}.mseed".format(event.name))
        client.get_waveforms_bulk(bulk_req, filename=filename)

    def __init__(self, remote_machine, config, s_file, recording_time):
        super(DataDownloader, self).__init__(remote_machine, config)
        self.event_info, self.iteration_info = utilities.get_lasif_information(
            self.remote_machine, self.config.lasif_project_path,
            self.config.base_iteration)
        self.stations_file = s_file
        self.recording_time = recording_time
        self.client = None

    def check_pre_staging(self):
        pass

    def stage_data(self):
        self.sf = pd.read_csv(self.stations_file)

        # Make a scratch directory for data.
        boltons.fileutils.mkdir_p("RAW_DATA")

    def check_post_staging(self):
        pass

    def run(self):

        length = self.recording_time * 60
        five_minutes = 5 * 60
        stations = [Station(sta=row.loc["Station"], net=row.loc["Network"])
                    for _, row in self.sf.iterrows()]
        events = [Event(name=key, start=info['origin_time']-five_minutes,
                        end=info['origin_time']+length) for
                  key, info in self.event_info.iteritems()]

        # Download things with ascyncio.
        workers = MAX_WORKERS
        with futures.ThreadPoolExecutor(MAX_WORKERS) as executor:
            executor.map(self._download_bulk, repeat(stations), events)
        # with futures.ThreadPoolExecutor(workers) as executor:
        #     executor.map(self._download, repeat(stations), events)
        # self._download(stations, events[0])

    def check_post_run(self):
        pass
