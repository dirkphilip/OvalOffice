import os

from oval_office_2 import utilities
from . import task
import obspy
import glob
import boltons.fileutils
import click


class SortCrossCorrelations(task.Task):
    """Sorts cross correlations into events. Saves the sorted data in 'NOISE_DATA'
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
        #x_new.write('./NOISE_DATA/GCMT_event_{}/raw/data.mseed'.format(station), format='MSEED')

    def __init__(self, remote_machine, config,correlations_dir):
        super(SortCrossCorrelations, self).__init__(remote_machine, config)
        self.event_info, self.iteration_info = utilities.get_lasif_information(
            self.remote_machine, self.config.lasif_project_path,
            self.config.base_iteration)
        self.event_list = list()
        self.st = obspy.Stream()
        self.hpass = 1 / self.iteration_info['highpass']
        self.lpass = 1 / self.iteration_info['lowpass']
        self.correlations_dir = correlations_dir
        self.all_events = sorted(self.event_info.keys())

    def check_pre_staging(self):
        pass

    def stage_data(self):
        noise_correlations = glob.glob(os.path.join(self.correlations_dir, "*"))

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

        print self.event_list


    def check_post_run(self):
        save_dir = os.path.join(self.config.lasif_project_path, 'DATA')
        with click.progressbar(self.all_events, label='Copying data to remote LASIF dir...') as events:
            for event in events:
                hpass = 1 / self.iteration_info['highpass']
                lpass = 1 / self.iteration_info['lowpass']
                event_dir = os.path.join(save_dir, event, 'preprocessed_{:.1f}_{:.1f}'.format(lpass, hpass))
                self.remote_machine.makedir(event_dir)

                pre_dat = os.path.join("NOISE_DATA", event, "preprocessed_{:.1f}_{:.1f}".format(self.lpass, self.hpass)
                                       , 'preprocessed_data.mseed')

                self.remote_machine.put_rsync(pre_dat, event_dir, verbose=True)
