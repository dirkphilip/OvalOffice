#!/users/afanasm/anaconda/bin/python
# -*- coding:utf-8 -*-

import cPickle
import multiprocessing
import os
from itertools import repeat

import numpy as np
import obspy

from oval_office_2.mini_lasif import LASIFAdjointSourceCalculationError
from oval_office_2.mini_lasif.ad_src_tf_phase_misfit import adsrc_tf_phase_misfit
from oval_office_2.mini_lasif.ad_src_cc_time_shift import adsrc_cc_time_shift


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
        print "Unable to read window/data/syn for {}".format(event)
        return (event, {}, {})

    station_dict = {}
    misfit_dict = {}
    for station, station_windows in event_windows.iteritems():

        network, station, component = station.split('.')
        station_data = data.select(network=network, station=station, channel='BH{}'.format(component),
                                   location='*')[0]
        station_synthetics = synthetics.select(network=network, station=station,
                                               channel='MX{}'.format(component))[0]

        # Scale data to synthetics.
        scale(station_data, station_synthetics)

        # Initialize adjoint source.
        misfit_val = 0.0
        adjoint_source_array = np.zeros_like(station_data.data)
        for window in station_windows:

            try:
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
            # Sometimes, when windows are at the very end of the trace, this function
            # will fail as the window falls off the end of the trace. This is because
            # the total running time for SPCEFEM changes whether save_Forward=true, or
            # save_forward=false.
            except Exception as e:
                print e
                print event, station, window
                continue

            try:
                adj_dict = adsrc_cc_time_shift(
                    time, window_data.data, window_synthetic.data, min_period, max_period)
                # adj_dict = adsrc_tf_phase_misfit(
                #     time, window_data.data, window_synthetic.data, min_period, max_period)
                adjoint_source_array += adj_dict['adjoint_source']
                misfit_val += adj_dict['misfit_value']
            except LASIFAdjointSourceCalculationError as e:
                print e

        misfit_dict['{}.{}.{}'.format(network, station, component)] = misfit_val
        station_dict['{}.{}.{}'.format(network, station, component)] = adjoint_source_array

    return (event, station_dict, misfit_dict)

def main():

    with open('lasif_data.p', 'rb') as fh:
        project_info = cPickle.load(fh)

    iteration_info = project_info[1]
    min_period = 1 / iteration_info['lowpass']
    max_period = 1 / iteration_info['highpass']

    pool = multiprocessing.Pool(processes=multiprocessing.cpu_count())
    all_sources = pool.map(windows_for_event, zip(project_info[0].keys(),
                                              repeat(min_period), repeat(max_period)))

    adjoint_sources = [(x[0], x[1]) for x in all_sources]
    misfits = [(x[0], x[2]) for x in all_sources]
    with open('adjoint_sources.p', 'wb') as fh:
        cPickle.dump(adjoint_sources, fh)
    with open('misfit.p', 'wb') as fh:
        cPickle.dump(misfits, fh)

if __name__ == "__main__":
    main()

