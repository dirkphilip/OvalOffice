import os

from oval_office_2 import utilities
from . import task
import obspy
import io
import click


class WriteNoiseEvents(task.Task):
    """Copies raw data from remote LASIF directory to OvalOffice directory.
    """

    def __init__(self, remote_machine, config):
        super(WriteNoiseEvents, self).__init__(remote_machine, config)
        self.event_info, self.iteration_info = utilities.get_lasif_information(
            self.remote_machine, self.config.lasif_project_path,
            self.config.base_iteration)
        self.event_list = list()
        self.data = None
        self.startTime = None

    def check_pre_staging(self):
        pass

    def stage_data(self):
        #TODO Decide where to keep the data
        self.data = obspy.read('./noise_correlations/*')
        self.startTime = self.data[0].stats.starttime
        for tr in self.data:
            netDotSta = tr.stats.network + '.' + tr.stats.station
            if netDotSta not in self.event_list:
                self.event_list.append(netDotSta)
        print self.event_list

    def generate_event_xml(self, event):

        station_xml_name = os.path.join(
                        "./NoiseXML",
                        "{}.{}.xml".format(event.split('.')[0], event.split('.')[1]))
        inv = obspy.read_inventory(station_xml_name,
                                                   format="stationxml")
        channel = inv.get_contents()['channels']

        print event

        print inv

        sta_loc = inv.get_coordinates(channel[0])

        with io.open(utilities.get_template_file('event'), 'rt') as fh:
            event_template = fh.read()

        return event_template.format(event_name=event,start_time=self.startTime, latitude=sta_loc['latitude'],
                                     longitude=sta_loc['longitude'], depth=sta_loc['local_depth'])

    def check_post_staging(self):
        pass

    def run(self):
        for event in self.event_list:

            event_string = self.generate_event_xml(event)
            cmt_path = os.path.join(self.config.lasif_project_path, 'EVENTS', 'GCMT_event_{}.xml'.format(event))
            self.remote_machine.write_file(cmt_path, str(event_string))


    def check_post_run(self):
        pass