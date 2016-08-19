import io
import os

import cPickle
import numpy as np
import obspy
import sys
import os
import math
import boltons.fileutils

from oval_office_2 import utilities
from . import task
import cPickle

class WeightAdjointSources(task.Task):
    """Weights the adjoint sources to decrease the effect of clusters on the kernel
        You still need to manually put the files in the right directory"""

    def is_in_list(self,listOfDicts, key, value):
        for dictionary in listOfDicts:
            if value == dictionary[key]:
                return True
        return False

    def __init__(self, remote_machine, config, step_length):
        super(WeightAdjointSources, self).__init__(remote_machine, config)
        self.event_info, self.iteration_info = utilities.get_lasif_information(
            self.remote_machine, self.config.lasif_project_path,
            self.config.base_iteration)
        self.all_events = sorted(self.event_info.keys())
        self.adjoint_sources_pickle = None
        self.stations_list = []
        self.adj_dir = None
        self.step_length = step_length

    def generate_stations_list(self):
        local_stations = "./STATION_XML_META/"
        for event in self.adjoint_sources_pickle:
            stations = event[1]
            for ii in stations.iteritems():
                net = ii[0].split(".")[0]
                sta = ii[0].split(".")[1]
                station_id = net + "." + sta

                if not self.is_in_list(self.stations_list,'station_id',station_id):
                    station_xml_name = os.path.join(local_stations,
                                                    "station.{}_{}.meta.xml".format(net, sta))
                    inv = obspy.read_inventory(station_xml_name, format="stationxml")
                    contents = inv.get_contents()['channels'][0]
                    station_dict = inv.get_coordinates(contents)
                    station_dict["station_id"] = station_id
                    self.stations_list.append(station_dict)

    def check_pre_staging(self):
        pass

    def get_inv_dist_weight(self, power=1):
        for elem in self.stations_list:
            inv_dist_weight = 0
            for each in self.stations_list:
                lat_dist = elem['latitude'] - each['latitude']
                lon_dist = elem['longitude'] - each['longitude']
                dist = math.sqrt(lat_dist ** 2 + lon_dist ** 2)
                if dist > 0.0001:
                    inv_dist_weight += 1/(dist ** power)
            elem['inv_dist_weight'] = inv_dist_weight

    def get_weight_listdicts(self,listofDicts,station_id, key):
        for dictionary in listofDicts:
            if dictionary['station_id'] == station_id:
                return dictionary[key]
        print "no weight found"
        return False

    def stage_data(self):
        src_dir = os.path.join(self.config.lasif_project_path, "ADJOINT_SOURCES_AND_WINDOWS/ADJOINT_SOURCES",
                               self.config.base_iteration, "*")
        self.adj_dir = os.path.join('./ADJOINT_SOURCES/', self.config.base_iteration)
        boltons.fileutils.mkdir_p(self.adj_dir)

        self.remote_machine.get_rsync(src_dir, self.adj_dir)

        if self.step_length:
            weight_dir = os.path.join('./ADJOINT_SOURCES/', "iter_1")
            weights_path =  os.path.join(weight_dir, "weights.p")
            with open(weights_path, 'rb') as fh:
                self.stations_list = cPickle.load(fh)
        else:
            with open(os.path.join(self.adj_dir, "adjoint_sources.p"), 'rb') as fh:
                self.adjoint_sources_pickle = cPickle.load(fh)
            self.generate_stations_list()
            self.get_inv_dist_weight()

    def check_post_staging(self):
        pass

    def run(self):

        if not self.step_length:
            for event, adj_sources in self.adjoint_sources_pickle:
                for station, adjoint_src in adj_sources.iteritems():
                    station_id = station.split(".")[0] + "." + station.split(".")[1]
                    weight = self.get_weight_listdicts(self.stations_list,station_id,'inv_dist_weight')
                    adj_sources[station] = adjoint_src / weight

            with open(os.path.join(self.adj_dir, "adjoint_sources_weighted.p"), 'wb') as fh:
                cPickle.dump(self.adjoint_sources_pickle, fh)

            with open(os.path.join(self.adj_dir, "weights.p"), 'wb') as fh:
                cPickle.dump(self.stations_list, fh)

        with open(os.path.join(self.adj_dir, "misfit.p"), 'rb') as fh:
            misfit_pickle = cPickle.load(fh)

        for event, misfits in misfit_pickle:
            for station in misfits:
                station_id = station.split(".")[0] + "." + station.split(".")[1]
                weight = self.get_weight_listdicts(self.stations_list,station_id,'inv_dist_weight')
                misfits[station] /= weight

        with open(os.path.join(self.adj_dir, "misfit_weighted.p"), 'wb') as fh:
            cPickle.dump(misfit_pickle, fh)


    def check_post_run(self):
        if not self.step_length:
            remote_adj_dir = os.path.join(self.config.lasif_project_path, "ADJOINT_SOURCES_AND_WINDOWS/ADJOINT_SOURCES",
                               self.config.base_iteration)
            command = "mv adjoint_sources.p adjoint_sources_unweighted.p"
            self.remote_machine.execute_command(command, workdir=remote_adj_dir)
            local_pickle = os.path.join(self.adj_dir, "adjoint_sources_weighted.p")
            self.remote_machine.put_rsync(local_pickle, remote_adj_dir)
            command = "mv adjoint_sources_weighted.p adjoint_sources.p"
            self.remote_machine.execute_command(command, workdir=remote_adj_dir)
        pass