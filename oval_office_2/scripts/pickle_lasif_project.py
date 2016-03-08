#!/usr/bin/env python
#! -*- coding: utf-8 -*-

import pickle
import sys

from lasif.components import project

iteration_name = sys.argv[1]
proj = project.Project('./', read_only_caches=True).comm.events.get_all_events()
lasif_iter = project.Project('./', read_only_caches=True).comm.iterations.get(iteration_name).get_process_params()
pickle_data = [proj, lasif_iter]
with open('./lasif_data.p', 'wb') as fh:
    pickle.dump(pickle_data, fh, protocol=-1)
