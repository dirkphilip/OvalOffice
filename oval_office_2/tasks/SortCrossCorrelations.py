import os

from oval_office_2 import utilities
from . import task
import obspy
import glob
import boltons.fileutils


class SortCrossCorrelations(task.Task):
    """Copies raw data from remote LASIF directory to OvalOffice directory.
    """

    def write_events(self, station):
        x_new = self.st.select(station=station.split('.')[1])

        boltons.fileutils.mkdir_p('./NOISE_DATA/GCMT_event_{}/preprocessed_{:.1f}_{:.1f}'
                                  .format(station, self.lpass, self.hpass))
        boltons.fileutils.mkdir_p('./NOISE_DATA/GCMT_event_{}/raw'.format(station))
        for tr in x_new:
            tr.stats.network = tr.stats.rec_station.split('.')[0]
            tr.stats.station = tr.stats.rec_station.split('.')[1]
            tr.stats.location = tr.stats.rec_station.split('.')[2]
        t1 = x_new[0].stats.starttime + float(self.config.simulation_time) * 60
        t2 = x_new[0].stats.endtime+10

        x_new.cutout(t1, t2)
        x_new.write('./NOISE_DATA/GCMT_event_{}/preprocessed_{:.1f}_{:.1f}/preprocessed_data.mseed'
                    .format(station, self.lpass, self.hpass), format='MSEED')
        x_new.write('./NOISE_DATA/GCMT_event_{}/raw/data.mseed'.format(station), format='MSEED')

    def __init__(self, remote_machine, config):
        super(SortCrossCorrelations, self).__init__(remote_machine, config)
        self.event_info, self.iteration_info = utilities.get_lasif_information(
            self.remote_machine, self.config.lasif_project_path,
            self.config.base_iteration)
        self.event_list = list()
        self.st = obspy.Stream()
        self.hpass = 1 / self.iteration_info['highpass']
        self.lpass = 1 / self.iteration_info['lowpass']

    def check_pre_staging(self):
        pass

    def stage_data(self):
        noise_correlations = glob.glob('./noise_correlations/*')

        for path in noise_correlations:
            rec_station =  path.split('/')[-1].split('-')[1]
            tr = obspy.read(path)
            tr[0].stats.rec_station = rec_station
            self.st.append(tr[0])

        for tr in self.st:
            event = tr.stats.network + '.' + tr.stats.station + '.' + tr.stats.location
            if event not in self.event_list:
                self.event_list.append(event)


    def check_post_staging(self):
        pass

    def run(self):
        for station in self.event_list:
            self.write_events(station)
        print "Sorted stations can be found in the NOISE_DATA directory"

    def check_post_run(self):
        pass