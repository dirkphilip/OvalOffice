# -*- coding: utf-8 -*-
"""
Project specific function picking windows.

:copyright:
    Lion Krischer (krischer@geophysik.uni-muenchen.de), 2013-2015
:license:
    GNU General Public License, Version 3
    (http://www.gnu.org/copyleft/gpl.html)
"""
import cPickle
import io
from itertools import repeat

from ipyparallel import Client


def iterate((event, event_info, iteration_info)):
    import os
    import obspy
    import cPickle
    from oval_office_2.mini_lasif.window_selection import select_windows

    print 'RUNNING {}'.format(event)

    def scale(dat, syn):

        scale_fac = syn.data.ptp() / dat.data.ptp()
        dat.stats.scaling_factor = scale_fac
        dat.data *= scale_fac

    def window_picking_function(data_trace, synthetic_trace, event_latitude,
                                event_longitude, event_depth_in_km,
                                station_latitude, station_longitude,
                                minimum_period, maximum_period,
                                **kwargs):  # NOQA
        """
        Function that will be called every time a window is picked. This is part
        of the project so it can change depending on the project.

        Please keep in mind that you will have to manually update this file to a
        new version if LASIF is ever updated.

        You can do whatever you want in this function as long as the function
        signature is honored and the correct data types are returned. You could
        for example only tweak the window picking parameters but you could also
        implement your own window picking algorithm or call some other tool that
        picks windows.

        This function has to return a list of tuples of start and end times,
        each tuple denoting a selected window.

        :param data_trace: Trace containing the fully preprocessed data.
        :type data_trace: :class:`~obspy.core.trace.Trace`
        :param synthetic_trace: Trace containing the fully preprocessed synthetics.
        :type synthetic_trace: :class:`~obspy.core.trace.Trace`
        :param event_latitude: The event latitude.
        :type event_latitude: float
        :param event_longitude: The event longitude.
        :type event_longitude: float
        :param event_depth_in_km: The event depth in km.
        :type event_depth_in_km: float
        :param station_latitude: The station latitude.
        :type station_latitude: float
        :param station_longitude: The station longitude.
        :type station_longitude: float
        :param minimum_period: The minimum period of data and synthetics.
        :type minimum_period: float
        :param maximum_period: The maximum period of data and synthetics.
        :type maximum_period: float
        """
        # Minimum normalised correlation coefficient of the complete traces.
        min_cc = 0.10

        # Maximum relative noise level for the whole trace. Measured from
        # maximum amplitudes before and after the first arrival.
        max_noise = 0.10

        # Maximum relative noise level for individual windows.
        max_noise_window = 0.4

        # All arrivals later than those corresponding to the threshold velocity
        # [km/s] will be excluded.
        min_velocity = 2.4

        # Maximum allowable time shift within a window, as a fraction of the
        # minimum period.
        threshold_shift = 0.30

        # Minimum normalised correlation coefficient within a window.
        threshold_correlation = 0.75

        # Minimum length of the time windows relative to the minimum period.
        min_length_period = 1.5

        # Minimum number of extrema in an individual time window (excluding the
        # edges).
        min_peaks_troughs = 2

        # Maximum energy ratio between data and synthetics within a time window.
        # Don't make this too small!
        max_energy_ratio = 10.0

        # The minimum similarity of the envelopes of both data and synthetics. This
        # essentially assures that the amplitudes of data and synthetics can not
        # diverge too much within a window. It is a bit like the inverse of the
        # ratio of both envelopes so a value of 0.2 makes sure neither amplitude
        # can be more then 5 times larger than the other.
        min_envelope_similarity = 0.2

        windows = select_windows(
            data_trace=data_trace,
            synthetic_trace=synthetic_trace,
            event_latitude=event_latitude,
            event_longitude=event_longitude,
            event_depth_in_km=event_depth_in_km,
            station_latitude=station_latitude,
            station_longitude=station_longitude,
            minimum_period=minimum_period,
            maximum_period=maximum_period,
            # User adjustable parameters.
            min_cc=min_cc,
            max_noise=max_noise,
            max_noise_window=max_noise_window,
            min_velocity=min_velocity,
            threshold_shift=threshold_shift,
            threshold_correlation=threshold_correlation,
            min_length_period=min_length_period,
            min_peaks_troughs=min_peaks_troughs,
            max_energy_ratio=max_energy_ratio,
            min_envelope_similarity=min_envelope_similarity,
            **kwargs)

        return windows

    specific = event_info[event]

    # data
    all_windows = {}
    try:
        data_stream = obspy.read(os.path.join(event, 'preprocessed_data.mseed'))
        synthetic_stream = obspy.read(os.path.join(event, 'synthetics.mseed'))
    except TypeError:
        print "Failed for {}".format(event)
        return event, {}
    except IOError:
        print "Couldn't read for {}".format(event)
        return event, {}
    print "PROCESSING {}".format(event)
    for i, dat_trace in enumerate(data_stream):

        print "PROCESSING {}".format(dat_trace)

        # read data
        starttime = dat_trace.stats.starttime
        location = dat_trace.stats.location
        network, station, channel = dat_trace.stats.network, dat_trace.stats.station, dat_trace.stats.channel[2]
        syn_trace = synthetic_stream.select(network=network, station=station, channel='MX{}'.format(channel))[0]

        # scale data
        scale(dat_trace, syn_trace)
        # read station information
        station_xml_file = os.path.join('StationXML', 'station.{}_{}.meta.xml'.format(network, station))
        try:
            inv = obspy.read_inventory(station_xml_file, format='stationxml')
            station_dict = inv.get_coordinates('{}.{}.{}.{}'.format(network, station, location, 'BHZ'),
                                               datetime=starttime)
        except Exception as e:
            print('SKIPPING {}.{}.{}'.format(network, station, channel))
            continue

        # pick windows
        windows = window_picking_function(dat_trace,
                                          syn_trace,
                                          specific['latitude'],
                                          specific['longitude'],
                                          specific['depth_in_km'],
                                          station_dict['latitude'],
                                          station_dict['longitude'],
                                          1 / iteration_info['lowpass'],
                                          1 / iteration_info['highpass'])

        if not windows:
            continue

        all_windows.update({'{}.{}.{}'.format(network, station, channel): windows})

    with open(os.path.join(event, 'windows.p'), 'wb') as fh:
        cPickle.dump(all_windows, fh)

    return event, all_windows


def main():

    with io.open('./lasif_data.p', 'rb') as fh:
        info = cPickle.load(fh)

    event_info = info[0]
    iteration_info = info[1]
    events = event_info.keys()

    # ipyparallel map
    client = Client()
    view = client[:]
    events_with_windows = view.map(iterate, zip(events, repeat(event_info), repeat(iteration_info)))
    results = events_with_windows.get()

    my_windows = dict(results)

    # Save
    with io.open('./windows_pickle.p', 'wb') as fh:
        cPickle.dump(my_windows, fh)


if __name__ == "__main__":
    main()
