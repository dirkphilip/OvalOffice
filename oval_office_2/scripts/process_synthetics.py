# -*- coding:utf-8 -*-

import multiprocessing
import os
import cPickle
from itertools import repeat

import numpy as np
import obspy
from obspy.signal.invsim import cosine_sac_taper


def process_synthetics(st, freqmax, freqmin):
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


if __name__ == '__main__':

    def loop((event, lowpass, highpass)):

        print "Processing synthetics for: " + event

        st = obspy.Stream()
        dst = os.path.join(event, 'OUTPUT_FILES', 'synthetics.mseed')
        srcs = os.path.join(event, 'OUTPUT_FILES')

        for seis_file in os.listdir(srcs):

            if not seis_file.endswith('.sac'):
                continue

            tr = obspy.read(os.path.join(srcs, seis_file))
            st += tr

        st = process_synthetics(st, lowpass, highpass)
        if len(st) > 0:
            print "WRITING TO " + dst
            st.write(dst, format='mseed')


    info = cPickle.load(open('info.p', 'rb'))
    freqmax_lasif = info['lowpass']
    freqmin_lasif = info['highpass']
    eventList = info['event_list']

    pool = multiprocessing.Pool(multiprocessing.cpu_count())
    pool.map(loop, zip(eventList, repeat(freqmax_lasif), repeat(freqmin_lasif)))
