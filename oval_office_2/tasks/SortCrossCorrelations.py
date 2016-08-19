import os

from oval_office_2 import utilities
from . import task
import obspy
import glob
import boltons.fileutils
import click
import cPickle

class SortCrossCorrelations(task.Task):
    """Sorts cross correlations into events. Saves the sorted data in 'NOISE_DATA'
    """

    def write_events(self, station):


        # fpr fixing patricks data
        starttime = self.synth[0].stats.starttime
        endtime = self.synth[0].stats.endtime

        x_new_temp = self.st.select(station=station.split('.')[1], network=station.split('.')[0])
        x_new = x_new_temp.copy()

        boltons.fileutils.mkdir_p('./NOISE_DATA/GCMT_event_{}/preprocessed_{:.1f}_{:.1f}'
                                  .format(station, self.lpass, self.hpass))
        boltons.fileutils.mkdir_p('./NOISE_DATA/GCMT_event_{}/raw'.format(station))
        for tr in x_new:
            #resampling
            tr = tr.interpolate(sampling_rate=1/0.1425)
            tr.stats.network = tr.stats.rec_station.split('.')[0]
            tr.stats.station = tr.stats.rec_station.split('.')[1]
            tr.stats.location = tr.stats.rec_station.split('.')[2]
        #t1 = x_new[0].stats.starttime +      2721.6075
        #t2 = x_new[0].stats.endtime+10


        #x_new.cutout(t1, t2)
        x_new.trim(starttime=starttime, endtime=endtime, pad=True, fill_value=0.0)
        x_new.write('./NOISE_DATA/GCMT_event_{}/preprocessed_{:.1f}_{:.1f}/preprocessed_data.mseed'
                    .format(station, self.lpass, self.hpass), format='MSEED')
        x_new.write('./NOISE_DATA/GCMT_event_{}/raw/data.mseed'.format(station), format='MSEED')

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
        self.synth = None

    def check_pre_staging(self):
        pass

    def stage_data(self):
        self.synth = obspy.read('./SYNTHETICS/GCMT_event_G.ATD.00/iter_1_noise/synthetics.mseed')
        noise_correlations = glob.glob(os.path.join(self.correlations_dir, "*"))

        for path in noise_correlations:
            rec_station = path.split('/')[-1].split('-')[1]
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

    def check_post_run(self):
        save_dir = os.path.join(self.config.lasif_project_path, 'DATA')
        with click.progressbar(self.all_events, label='Copying data to remote LASIF dir...') as events:
            for event in events:
                hpass = 1 / self.iteration_info['highpass']
                lpass = 1 / self.iteration_info['lowpass']
                preproc_event_dir = os.path.join(save_dir, event, 'preprocessed_{:.1f}_{:.1f}'.format(lpass, hpass))
                raw_event_dir = os.path.join(save_dir, event, 'raw')
                self.remote_machine.makedir(preproc_event_dir)
                self.remote_machine.makedir(raw_event_dir)

                pre_dat = os.path.join("NOISE_DATA", event, "preprocessed_{:.1f}_{:.1f}".format(self.lpass, self.hpass)
                                       , 'preprocessed_data.mseed')
                raw_dat = os.path.join("NOISE_DATA", event, "raw", 'data.mseed')

                self.remote_machine.put_rsync(pre_dat, preproc_event_dir)
                self.remote_machine.put_rsync(raw_dat, raw_event_dir)
