
import cPickle

import io
import os


def test_generate_cmt_string():

    from ..

    this_dir = os.path.dirname(__file__)
    with io.open(os.path.join(this_dir, "data", "lasif_data.p"), "rb") as fh:
        info, _ = cPickle.load(fh)

    with io.open(os.path.join(this_dir, "data", "CMTSOLUTION")) as fh:
        true_file = fh.read()

    g = GenerateCmtSolutions.generate_cmt_string(
        info['GCMT_event_ALASKA_PENINSULA_Mag_5.7_2011-11-6-8'])

    assert g == true_file
