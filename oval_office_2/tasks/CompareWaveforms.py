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
import boltons.fileutils
import sys
from PyQt4 import QtGui
import matplotlib.pyplot as plt
from matplotlib.backends.backend_qt4agg import FigureCanvasQTAgg as FigureCanvas




class CompareWaveforms(task.Task):
    """Run copy_mseeds first, this will plot synthethic and preprocessed waveforms"

    """

    def __init__(self, remote_machine, config):
        super(CompareWaveforms, self).__init__(remote_machine, config)
        self.event_info, self.iteration_info = utilities.get_lasif_information(
            self.remote_machine, self.config.lasif_project_path,
            self.config.base_iteration)
        self.synthetics   = None
        self.preproc_data = None
        self.window       = None
        self.station_idx  = 0
        self.event_idx = 0
        self.show_prev_iteration = False  # Can be set in the UI


    def read(self, event):
        synth_path = os.path.join("SYNTHETICS", event, self.config.base_iteration, "synthetics.mseed")

        if self.show_prev_iteration:
            try:
                synth_prev_path = os.path.join("SYNTHETICS", event, self.config.prev_iteration, "synthetics.mseed")
                self.synthetics_prev = obspy.read(synth_prev_path)
            except:
                print 'Cannot find previous iteration, please check config.json and if files are present'
        preproc_path = os.path.join("PREPROC_DATA", event, self.config.base_iteration, "preprocessed_data.mseed")
        window_path = os.path.join("WINDOWS", event, self.config.first_iteration, "windows.p")
        self.synthetics = obspy.read(synth_path)
        self.preproc_data = obspy.read(preproc_path)

        with open(window_path, 'rb') as fh:
            self.window = cPickle.load(fh)

    def plotWaveform(self,preproc, synth, station,event,fig, synth_prev=None, update=False):
        base = mdates.date2num(preproc[0].stats.starttime.datetime)
        end = mdates.date2num(preproc[0].stats.endtime.datetime)
        daterange = np.linspace(base, end, preproc[0].data.size)

        plt.clf()
        ax = fig.add_subplot(111)
        ax.hold(False)

        ax.set_title(event + " \n" + str(station) + " at time " +
                     str(preproc[0].stats.starttime.datetime) + " - " + str(preproc[0].stats.endtime.datetime))
        ax.hold(True)
        plt.ylabel('Amplitude')
        plt.xlabel('Time')

        ax.plot_date(daterange,preproc[0].data, marker='', ls='-', color='black', label='Preprocessed Data')
        ax.plot_date(daterange,synth[0].data, marker='', ls='--', color='red', label=self.config.base_iteration)
        if self.show_prev_iteration:
            ax.plot_date(daterange,synth_prev[0].data, marker='', ls='--', color='green', label=self.config.prev_iteration)

        ax.xaxis.set_minor_locator(mdates.MinuteLocator(np.arange(0,60,1)))
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
        ax.fmt_xdata = mdates.DateFormatter('%Y-%m-%d %H:%M:%S')
        ax.legend(loc='lower right')


        for pair in self.window[station]:
            start = mdates.date2num(pair[0].datetime)
            end = mdates.date2num(pair[1].datetime)
            width = end - start
            rect = patches.Rectangle((start, 10*min(preproc[0].data)), width, 100*max(preproc[0].data), color='lightcoral')
            ax.add_patch(rect)

        if update is False:
            plt.show()

        return fig

    def check_pre_staging(self):
        pass

    def stage_data(self):
        pass

    def check_post_staging(self):
        pass

    def run(self):
        all_events = sorted(self.event_info.keys())

        for event in all_events[:]:
            self.read(event)

            for station in self.window.keys()[:3]:
                print station
                netw = station.split(".", 2)[0]
                sta  = station.split(".", 2)[1]
                cha = station.split(".", 2)[2]

                # Extract data based on picked windows
                synth = self.synthetics.select(id="{}.{}.S*.MX{}".format(netw, sta, cha))
                preproc = self.preproc_data.select(id="{}.{}.*.*H{}".format(netw, sta, cha))
                if self.config.input_data_type == 'noise':
                    t1 = preproc[0].stats.endtime
                    t2 = synth[0].stats.endtime + 10
                    synth.cutout(t1,t2)
                #Normalize preprocessed data
                print self.preproc_data[0]
                preproc.data = preproc[0].normalize(norm=max(abs(preproc[0].data))/max(abs(synth[0].data)))

                # Plot
                fig = plt.figure(figsize=(12,6))

                if self.show_prev_iteration:
                    synth_prev = self.synthetics_prev.select(id="{}.{}.S3.MX{}".format(netw, sta, cha))
                    self.plotWaveform(preproc,synth,station,event, fig, synth_prev)
                else:
                    self.plotWaveform(preproc,synth,station,event, fig)


    def updateFigure(self, fig, next_station=False, prev_station=False, next_event=False, prev_event=False ):
        all_events = sorted(self.event_info.keys())
        max_event = len(all_events)

        if next_event:
            self.event_idx += 1
            self.station_idx = 0
            if self.event_idx + 1 == max_event:
                self.event_idx = 0
        if prev_event:
            self.event_idx -= 1
            self.station_idx = 0
            if self.event_idx < 0:
                self.event_idx = max_event

        event = all_events[self.event_idx]
        self.read(event)

        max_station = len(self.window.keys())
        if max_station == 0:
            print "No windows found for:", event ,", try a different event"
            return fig

        if next_station:
            self.station_idx += 1
            if self.station_idx + 1 == max_station:
                self.station_idx = 0

        if prev_station:
            self.station_idx -= 1
            if self.station_idx < 0:
                self.station_idx = max_station

        station = self.window.keys()[self.station_idx]
        netw = station.split(".", 2)[0]
        sta  = station.split(".", 2)[1]
        cha = station.split(".", 2)[2]

        # Extract data based on picked windows
        synth = self.synthetics.select(id="{}.{}.S*.MX{}".format(netw, sta, cha))
        preproc = self.preproc_data.select(id="{}.{}.*.*H{}".format(netw, sta, cha))
        if self.config.input_data_type == 'noise':
            t1 = preproc[0].stats.endtime
            t2 = synth[0].stats.endtime + 10
            synth.cutout(t1,t2)

        #Normalize preprocessed data
        preproc.data = preproc[0].normalize(norm=max(abs(preproc[0].data))/max(abs(synth[0].data)))

        if self.show_prev_iteration:
            synth_prev = self.synthetics_prev.select(id="{}.{}.S3.MX{}".format(netw, sta, cha))
            fig = self.plotWaveform(preproc,synth,station,event, fig, synth_prev, update=True)
        else:
            fig = self.plotWaveform(preproc,synth,station,event, fig, update=True)

        return fig

    def setShowPrevious(self):
        if self.show_prev_iteration:
            self.show_prev_iteration = False
        else:
            self.show_prev_iteration = True


    def check_post_run(self):
        pass


class Example(QtGui.QWidget):

    def __init__(self,system,config):
        super(Example, self).__init__()

        self.task = CompareWaveforms(system,config)
        self.initUI()
        self.showPrevioursIteration = False

    def initUI(self):
        self.figure = plt.figure()
        self.canvas = FigureCanvas(self.figure)

        self.setGeometry(1600, 1600, 1600, 500)
        self.setWindowTitle('Waveform inspection')

        self.button_next_station = QtGui.QPushButton('Next station')
        self.button_prev_station = QtGui.QPushButton('Previous station')
        self.button_next_event = QtGui.QPushButton('Next event')
        self.button_prev_event = QtGui.QPushButton('Previous event')
        self.button_save = QtGui.QPushButton('Save')
        self.button_show_prev = QtGui.QPushButton('Show Last Iteration')

        layout = QtGui.QVBoxLayout()

        hbox0 = QtGui.QHBoxLayout()
        hbox0.addWidget(self.button_prev_station)
        hbox0.addWidget(self.button_next_station)

        hbox1 = QtGui.QHBoxLayout()
        hbox1.addWidget(self.button_prev_event)
        hbox1.addWidget(self.button_next_event)

        hbox2 = QtGui.QHBoxLayout()
        hbox2.addWidget(self.button_save)
        hbox2.addWidget(self.button_show_prev)

        layout.addWidget(self.canvas)
        layout.addLayout(hbox0)
        layout.addLayout(hbox1)
        layout.addLayout(hbox2)

        self.setLayout(layout)
        self.button_next_event.clicked.connect(self.nextEvent)
        self.button_prev_event.clicked.connect(self.prevEvent)
        self.button_next_station.clicked.connect(self.nextStation)
        self.button_prev_station.clicked.connect(self.prevStation)
        self.button_save.clicked.connect(self.saveFig)
        self.button_show_prev.clicked.connect(self.showPrevious)

        self.show()
        self.setFigure(self.task.updateFigure(self.figure))


    def showPrevious(self):
        self.task.setShowPrevious()
        self.setFigure(self.task.updateFigure(self.figure))

    def nextStation(self):
        self.setFigure(self.task.updateFigure(self.figure, next_station=True))

    def prevStation(self):
        self.setFigure(self.task.updateFigure(self.figure, prev_station=True))

    def nextEvent(self):
        self.setFigure(self.task.updateFigure(self.figure, next_event=True))

    def prevEvent(self):
        self.setFigure(self.task.updateFigure(self.figure, prev_event=True))

    def saveFig(self):
        boltons.fileutils.mkdir_p('FIGURES')
        filenum = len(os.listdir('./FIGURES'))

        self.figure.savefig('./FIGURES/myfig_{}'.format(filenum))

    def setFigure(self, fig):
        self.figure = fig
        self.canvas.draw()

