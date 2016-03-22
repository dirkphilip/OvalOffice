# -*- coding:utf-8 -*-
import cPickle
import io
import os
from itertools import repeat

import numpy as np
import obspy
from multiprocessing import Pool, cpu_count
from obspy.signal.invsim import cosine_sac_taper


def process_synthetics(st, freqmax, freqmin):
    """Process syntetics function snagged from LASIF."""

    f2 = 0.9 * freqmin
    f3 = 1.1 * freqmax
    f1 = 0.5 * f2
    f4 = 2.0 * f3
    pre_filt = (f1, f2, f3, f4)

    # Detrend and taper.
    st.detrend("linear")
    st.detrend("demean")
    st.taper(max_percentage=0.05, type="hann")

    # Perform a frequency domain taper like during the response removal
    # just without an actual response...
    for tr in st:
        tr.differentiate()

        data = tr.data.astype(np.float64)
        orig_len = len(data)

        # smart calculation of nfft dodging large primes
        # noinspection PyProtectedMember
        from obspy.signal.util import _npts2nfft
        nfft = _npts2nfft(len(data))

        fy = 1.0 / (tr.stats.delta * 2.0)
        freqs = np.linspace(0, fy, nfft // 2 + 1)

        # Transform data to Frequency domain
        data = np.fft.rfft(data, n=nfft)
        # noinspection PyTypeChecker
        data *= cosine_sac_taper(freqs, flimit=pre_filt)
        data[-1] = abs(data[-1]) + 0.0j

        # transform data back into the time domain
        data = np.fft.irfft(data)[0:orig_len]

        # assign processed data and store processing information
        tr.data = data

        tr.detrend("linear")
        tr.detrend("demean")
        tr.taper(0.05, type="cosine")
        tr.filter("bandpass", freqmin=freqmin, freqmax=freqmax, corners=3, zerophase=True)
        tr.detrend("linear")
        tr.detrend("demean")
        tr.taper(0.05, type="cosine")
        tr.filter("bandpass", freqmin=freqmin, freqmax=freqmax, corners=3, zerophase=True)

    return st


def process_one_event((event, lowpass, highpass)):
    """Process the files for a single event.
    :param highpass: Highpass filter frequency.
    :param lowpass: Lowpass filter frequency.
    :param event: Event name.
    """

    st = obspy.Stream()
    dst = os.path.join(event, 'OUTPUT_FILES', 'synthetics.mseed')
    srcs = os.path.join(event, 'OUTPUT_FILES')

    print "PROCESSING: " + event
    for seis_file in os.listdir(srcs):

        if not seis_file.endswith('.sac'):
            continue

        tr = obspy.read(os.path.join(srcs, seis_file))
        st += tr

    if len(st) > 0:
        st = process_synthetics(st, lowpass, highpass)
    if len(st) > 0:
        st.write(dst, format='mseed')


if __name__ == '__main__':

    # Read in iteration information.
    with io.open('info.p', 'rb') as fh:
        info = cPickle.load(fh)
    freqmax_lasif = info['lowpass']
    freqmin_lasif = info['highpass']
    eventList = info['event_list']

    # Execute in parallel.
    pool = Pool(processes=cpu_count())
    pool.map(process_one_event, zip(eventList, repeat(freqmax_lasif), repeat(freqmin_lasif)),
             chunksize=len(eventList)/cpu_count())
