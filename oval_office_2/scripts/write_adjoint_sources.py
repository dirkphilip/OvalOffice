#!/usr/bin/env python
# -*- coding: utf-8 -*-

import cPickle
import io
from itertools import repeat
from multiprocessing import Pool, cpu_count


def iterate((event, windows, iteration_info)):
    import os
    import numpy as np
    specfem_delta_delay = -1.0687500

    print "WRITING SOURCE FOR {}".format(event)

    # Make time array to save.
    dt = iteration_info['dt']
    npts = iteration_info['npts']
    iterable = (specfem_delta_delay + i * dt for i in range(npts))
    timearray = np.fromiter(iterable, dtype=float)

    event_set = set(windows.keys())
    event_dir = os.path.join(event)
    for window, values in windows.iteritems():
        for comp in ['E', 'N', 'Z']:

            sta, net = window.split('.')[:2]
            set_string = '{}.{}.{}'.format(sta, net, comp)
            adjoint_source_file_string = '{}.{}.MX{}.adj'.format(sta, net, comp)
            adjoint_source_file = os.path.join(event_dir, 'SEM', adjoint_source_file_string)

            if set_string in event_set:

                # Note here that adjoint sources are different (reversed) in SES3d as they are in SPECFEM!!!
                np.savetxt(adjoint_source_file,
                           np.transpose([timearray, -1 * values[::-1]]), fmt='%.6e %.6e')
            else:
                np.savetxt(adjoint_source_file,
                           np.transpose([timearray, np.zeros(npts)]), fmt='%.6e %.6e')

            event_stations_file_path = os.path.join(event_dir, 'DATA', 'STATIONS')
            event_adjoint_stations_file_path = os.path.join(event_dir, 'DATA', 'STATIONS_ADJOINT')

            with io.open(event_stations_file_path, 'r') as fh:
                lines = fh.readlines()

            adjoint_lines = set()
            for line in lines:
                sta_net = '.'.join(line.split()[:2][::-1])
                for all_comp in ['Z', 'E', 'N']:
                    if sta_net + '.{}'.format(all_comp) in event_set:
                        adjoint_lines.add(line)

            with io.open(event_adjoint_stations_file_path, 'w') as fh:
                fh.writelines(line for line in adjoint_lines)


def main():
    with io.open('project_pickle.p', 'rb') as fh:
        event_info, iteration_info = cPickle.load(fh)

    with io.open('adjoint_sources.p', 'rb') as fh:
        adjoint_sources = cPickle.load(fh)

    events, sources = zip(*adjoint_sources)

    pool = Pool(cpu_count())
    pool.map(iterate, zip(events, sources, repeat(iteration_info)))


if __name__ == '__main__':
    main()
