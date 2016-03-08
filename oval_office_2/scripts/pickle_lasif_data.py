#! -*- coding: utf-8 -*-

import os
import pickle

import obspy

all_metadata = []
for dirpath, subdirs, files in os.walk('./'):
    if 'raw' not in dirpath:
        continue
    if files[0] != 'data.mseed':
        continue
    st = obspy.read(os.path.join(dirpath, 'data.mseed'))
    for tr in st:
        all_metadata.append((tr.stats.network, tr.stats.station, tr.stats.location))

all_metadata = set(all_metadata)
pickle.dump(all_metadata, open('./data_pickle.p', 'wb'))
