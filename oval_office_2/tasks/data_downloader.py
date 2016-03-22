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
MAX_WORKERS = 20

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
                stream += client.get_waveforms(
                    *req, quality='B', minimumlength=event.end-event.start)
            except FDSNException as e:
                pass

        fname = os.path.join("RAW_DATA", "{}.mseed".format(event.name))
        stream.write("{}.mseed".format(event.name), format='mseed')

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
                  key, info in self.event_info.iteritems()][:5]

        # Download things with ascyncio.
        workers = MAX_WORKERS
        with futures.ThreadPoolExecutor(workers) as executor:
            executor.map(self._download, repeat(stations), events)

    def check_post_run(self):
        pass