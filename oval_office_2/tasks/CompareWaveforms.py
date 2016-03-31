import os

from oval_office_2 import utilities
from . import task
import matplotlib.pyplot as plt
import click
import obspy
import datetime
import numpy as np
import matplotlib.patches as patches
import matplotlib.dates as mdates
import cPickle


class CompareWaveforms(task.Task):
    """Run copy_mseeds first, this will plot synthethic and preprocessed waveforms"

    """

    # TODO ADD PICKLE THING FOR TIME WINDOW PLOTTING

    def __init__(self, remote_machine, config):
        super(CompareWaveforms, self).__init__(remote_machine, config)
        self.event_info, self.iteration_info = utilities.get_lasif_information(
            self.remote_machine, self.config.lasif_project_path,
            self.config.base_iteration)
        self.synthetics   = None
        self.preproc_data = None
        self.window       = None

    def read(self, event):
        synth_path = os.path.join("SYNTHETICS", event, "synthetics.mseed")
        preproc_path = os.path.join("PREPROC_DATA", event, "preprocessed_data.mseed")
        window_path  = os.path.join("WINDOWS", event, "windows.p")
        self.synthetics = obspy.read(synth_path)
        self.preproc_data = obspy.read(preproc_path)

        with open(window_path, 'rb') as fh:
	        self.window = cPickle.load(fh)

    def plotWaveform(self,preproc,synth,station,event):

        base = preproc[0].stats.starttime.datetime
        end  = preproc[0].stats.endtime.datetime
        delta = (end-base)/(preproc[0].stats.npts-1)
        #delta = datetime.timedelta(seconds=1/preproc[0].stats.sampling_rate)
        l = mdates.drange(base, end, delta)

        if l.size != preproc[0].data.size:
            print l.size
            print delta
            print end-base
            print preproc[0].stats.npts


        fig = plt.figure(figsize=(12,6))

        ax = fig.add_subplot(111)
        #ax.set_title(str(station) + " at time " + str(preproc[0].stats.starttime.datetime) + " - " + str(preproc[0].stats.endtime.datetime))
        ax.set_title(event + " \n" + str(station) + " at time " + str(preproc[0].stats.starttime.datetime) + " - " + str(preproc[0].stats.endtime.datetime))

        ax.plot_date(l,preproc[0].data, marker='', ls='-', color='black', label='Preprocessed')
        ax.plot_date(l,synth[0].data, marker='', ls='--', color='red', label='Synthetics')
        #ax.xaxis.set_major_locator(mdates.MinuteLocator(np.arange(0,60,10)))
        td = (end-base)/3
        #ax.xaxis.set_major_locator(mdates.DateLocator(mdates.drange(base, end, td)))
        ax.xaxis.set_minor_locator(mdates.MinuteLocator(np.arange(0,60,1)))
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
        ax.fmt_xdata = mdates.DateFormatter('%Y-%m-%d %H:%M:%S')
        ax.legend(loc='lower right')


        for pair in self.window[station]:
            start = mdates.date2num(pair[0].datetime)
            end = mdates.date2num(pair[1].datetime)
            width = end - start
            rect = patches.Rectangle((start, 10*min(preproc[0].data)), width, 100*max(preproc[0].data), color='lightgrey')
            ax.add_patch(rect)


        plt.show()

    def check_pre_staging(self):
        pass

    def stage_data(self):
        pass

    def check_post_staging(self):
        pass

    def run(self):
        all_events = sorted(self.event_info.keys())
        for event in all_events[3:5]:
            self.read(event)

            for station in self.window.keys():
                netw = station.split(".", 2)[0]
                sta  = station.split(".", 2)[1]
                cha = station.split(".", 2)[2]

                # Extract data based on picked windows
                synth = self.synthetics.select(id="{}.{}.S3.MX{}".format(netw, sta, cha))
                preproc = self.preproc_data.select(id="{}.{}.*.BH{}".format(netw, sta, cha))


                #Normalize preprocessed data
                preproc.data = preproc[0].normalize(norm=max(abs(preproc[0].data))/max(abs(synth[0].data)))

                # Plot
                self.plotWaveform(preproc,synth,station,event)

            # for tr in self.preproc_data[:1]:  # [:1] for testing purposes TODO remove
            #     sta = tr.stats.station
            #     cha = "MX" + tr.stats.channel[-1]
            #
            #     try:
            #         synth = self.synthetics.select(station=sta).select(channel=cha)
            #         tr = tr.normalize(norm=max(abs(tr.data))/max(abs(synth[0].data)))
            #
            #         plt.figure()
            #         plt.title(sta + " " + cha)
            #         plt.plot(tr.data, label='preprocessed data')
            #         plt.plot(synth[0].data, label='synthethics')
            #         plt.grid()
            #         plt.legend(loc='lower right')
            #         plt.show()
            #     except:
            #         print "Station " + sta + "."+ cha + " could not be found in synthetic data"



    def check_post_run(self):
        pass
