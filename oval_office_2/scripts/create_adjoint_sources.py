#!/users/afanasm/anaconda/bin/python
# -*- coding:utf-8 -*-

import cPickle
import multiprocessing
import os
from itertools import repeat

import numpy as np
import obspy
from . import LASIFAdjointSourceCalculationError
from oval_office_2.mini_lasif.ad_src_tf_phase_misfit import adsrc_tf_phase_misfit

def scale(dat, syn):
    scale_fac = syn.data.ptp() / dat.data.ptp()
    dat.stats.scaling_factor = scale_fac
    dat.data *= scale_fac


def windows_for_event((event, min_period, max_period)):

    print "Generating adjoint source for {}".format(event)

    try:
        with open(os.path.join(event, 'windows.p'), 'rb') as fh:
            event_windows = cPickle.load(fh)
            synthetics = obspy.read(os.path.join(event, 'synthetics.mseed'))
            data = obspy.read(os.path.join(event, 'preprocessed_data.mseed'))
    except:
        print "Unable to create windows for {}".format(event)
        return (event, {})

    station_dict = {}
    for station, station_windows in event_windows.iteritems():

        network, station, component = station.split('.')
        station_data = data.select(network=network, station=station, channel='BH{}'.format(component),
                                   location='*')[0]
        station_synthetics = synthetics.select(network=network, station=station,
                                               channel='MX{}'.format(component))[0]

        # Scale data to synthetics.
        scale(station_data, station_synthetics)

        # Initialize adjoint source.
        adjoint_source_array = np.zeros_like(station_data.data)
        for window in station_windows:

            start, end = window
            samples_from_start = int(round((start - station_data.stats.starttime) / station_data.stats.delta))
            samples_from_end = int(round((end - station_data.stats.starttime) / station_data.stats.delta + 1))
            window_slice = slice(samples_from_start, samples_from_end)

            window_data = station_data.slice(
                *window, nearest_sample=True).taper(max_percentage=0.10, type='cosine')
            window_synthetic = station_synthetics.slice(
                *window, nearest_sample=True).taper(max_percentage=0.10, type='cosine')
            time = np.linspace(0, station_data.stats.npts * station_data.stats.delta, station_data.stats.npts)

            window_data.trim(station_data.stats.starttime, station_data.stats.endtime,
                             pad=True, fill_value=0.0)
            window_synthetic.trim(station_data.stats.starttime, station_data.stats.endtime,
                             pad=True, fill_value=0.0)

            try:
                adj_dict = adsrc_tf_phase_misfit(
                    time, window_data.data, window_synthetic.data, min_period, max_period)
                adjoint_source_array += adj_dict['adjoint_source']
            except LASIFAdjointSourceCalculationError as e:
                print e

        station_dict['{}.{}.{}'.format(network, station, component)] = adjoint_source_array

    return (event, station_dict)

def main():

    with open('project_pickle.p', 'rb') as fh:
        project_info = cPickle.load(fh)

    iteration_info = project_info[1]
    min_period = 1 / iteration_info['lowpass']
    max_period = 1 / iteration_info['highpass']

    pool = multiprocessing.Pool(processes=multiprocessing.cpu_count())
    all_windows = pool.map(windows_for_event, zip(project_info[0].keys(),
                                              repeat(min_period), repeat(max_period)))

    with open('adjoint_sources.p', 'wb') as fh:
        cPickle.dump(all_windows, fh)

if __name__ == "__main__":
    main()

