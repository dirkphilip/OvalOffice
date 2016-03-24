import cPickle
import io
import os

import obspy
import pytest
import numpy as np

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

def test_preprocessing(lasif_info):

    from oval_office_2.scripts import preprocess_data

    work_path = os.path.join(PATH, 'data', 'preprocessing')
    os.chdir(work_path)

    test_event = 'GCMT_event_ALASKA_PENINSULA_Mag_5.7_2011-11-6-8'
    preprocess_data._loop((test_event, lasif_info))

    tr1 = obspy.read(os.path.join(test_event, 'AAK.II..BHZ.mseed'))[0]
    tr2 = obspy.read(os.path.join(test_event, 'preprocessed_data.mseed'))[0]

    np.testing.assert_allclose(tr2.data, tr1.data)

    os.remove(os.path.join(test_event, 'preprocessed_data.mseed'))
