import cPickle
import io
import os

import xml.etree.ElementTree as ET
import obspy
import pytest
import numpy as np

TEST_EVENT = 'GCMT_event_ALASKA_PENINSULA_Mag_5.7_2011-11-6-8'
PATH = os.path.dirname(os.path.realpath(__file__))

@pytest.fixture
def lasif_info():

    import cPickle
    test_file = os.path.join(PATH, 'data', 'lasif_data.p')
    with io.open(test_file, 'rb') as fh:
        return cPickle.load(fh)


def test_generate_cmt_string():

    from oval_office_2.tasks import GenerateCmtSolutions

    this_dir = os.path.dirname(__file__)
    with io.open(os.path.join(this_dir, "data", "lasif_data.p"), "rb") as fh:
        info, _ = cPickle.load(fh)

    with io.open(os.path.join(this_dir, "data", "CMTSOLUTION")) as fh:
        true_file = fh.read()

    g = GenerateCmtSolutions.GenerateCmtSolutions.generate_cmt_string(
        info['GCMT_event_ALASKA_PENINSULA_Mag_5.7_2011-11-6-8'])

    assert g == true_file

def test_data_processing(lasif_info):

    from oval_office_2.scripts import preprocess_data

    work_path = os.path.join(PATH, 'data', 'preprocessing')
    os.chdir(work_path)

    preprocess_data._loop((TEST_EVENT, lasif_info))

    tr1 = obspy.read(os.path.join(TEST_EVENT, 'AAK.II..BHZ.mseed'))[0]
    tr2 = obspy.read(os.path.join(TEST_EVENT, 'preprocessed_data.mseed'))[0]

    np.testing.assert_allclose(tr2.data, tr1.data)

    os.remove(os.path.join(TEST_EVENT, 'preprocessed_data.mseed'))

def test_synthetic_processing(lasif_info):

    from oval_office_2.scripts import process_synthetics
    work_path = os.path.join(PATH, 'data', 'synthetics')
    os.chdir(work_path)

    lowpass = lasif_info[1]['lowpass']
    highpass = lasif_info[1]['highpass']
    process_synthetics._loop((TEST_EVENT, lowpass, highpass))

    tr1 = obspy.read(os.path.join(TEST_EVENT, 'processed_reference.mseed'))[0]
    tr2 = obspy.read(os.path.join(TEST_EVENT, 'OUTPUT_FILES', 'synthetics.mseed'))[0]

    np.testing.assert_allclose(tr1.data, tr2.data, rtol=1e-5)

    os.remove(os.path.join(TEST_EVENT, 'OUTPUT_FILES', 'synthetics.mseed'))

def test_select_windows(lasif_info):

    from oval_office_2.scripts import select_windows
    work_path = os.path.join(PATH, 'data', 'window_selection')
    os.chdir(work_path)

    e, w = select_windows.iterate((TEST_EVENT, lasif_info[0], lasif_info[1]))

    # Reference
    tree = ET.parse('window_II.AAK.00.BHZ.xml')
    root = tree.getroot()
    s_times, e_times = [], []
    for child in root.iter('Window'):
        for st in child.iter('Starttime'):
            s_times.append(obspy.UTCDateTime(st.text))
        for st in child.iter('Endtime'):
            e_times.append(obspy.UTCDateTime(st.text))

    # Calculated
    with io.open(os.path.join(TEST_EVENT, 'windows.p'), 'rb') as fh:
        windows = cPickle.load(fh)

    s_time_calc = [w[0] for w in windows['II.AAK.Z']]
    e_time_calc = [w[1] for w in windows['II.AAK.Z']]
    assert (s_time_calc == s_times)
    assert (e_time_calc == e_times)

    os.remove(os.path.join(TEST_EVENT, 'windows.p'))





