import io
import os

import click

from .. import utilities
from . import task

class GenerateCmtSolutions(task.Task):
    """Generates the CMT solutions in specfem3d_globe format.

    Reads in the CMT solution template, and populates it with event-specific
    parameters. Then, copies the formatted files to the correct directories
    on the remote machine.
    """

    def __init__(self, remote_machine, config):
        super(GenerateCmtSolutions, self).__init__(remote_machine, config)
        self.event_info, self.iteration_info = utilities.get_lasif_information(
            self.remote_machine, self.config.lasif_project_path,
            self.config.base_iteration)


    @staticmethod
    def generate_cmt_string_for_noise_events(event_info):
        """Generates a specfem CMTSOLUTION given an oval_office config object.

        :param event_info: Oval office event dictionary.
        :type config: dict
        :returns String containing formatted CMT solution.
        """
        with io.open(utilities.get_template_file('CMT_solution'), 'rt') as fh:
            cmt_template = fh.read()

        start_time = event_info['origin_time']
        longitude = event_info['longitude']
        latitude = event_info['latitude']
        depth = event_info['depth_in_km']
        mag = 0
        mrr = 0
        mtt = 0
        mpp = 0
        mrt = 0
        mrp = 0
        mtp = 0
        hdur = 1 #also used for shifting time

        # Format.
        return cmt_template.format(
            time_year=start_time.year,
            time_month=start_time.month,
            time_day=start_time.day,
            time_hh=start_time.hour,
            time_mm=start_time.minute,
            time_ss=start_time.second + hdur*5 + start_time.microsecond / 1e6,
            event_mag=mag,
            event_name=str(start_time) + '_' + ('%1.f' % mag),
            event_latitude=float(latitude),
            event_longitude=float(longitude),
            event_depth=float(depth),
            half_duration=hdur,
            time_shift=0.0,
            mtt=mtt,
            mrr=mrr,
            mpp=mpp,
            mtp=mtp,
            mrt=mrt,
            mrp=mrp)


    @staticmethod
    def generate_cmt_string(event_info):
        """Generates a specfem CMTSOLUTION given an oval_office config object.

        :param event_info: Oval office event dictionary.
        :type config: dict
        :returns String containing formatted CMT solution.
        """
        with io.open(utilities.get_template_file('CMT_solution'), 'rt') as fh:
            cmt_template = fh.read()

        start_time = event_info['origin_time']
        longitude = event_info['longitude']
        latitude = event_info['latitude']
        depth = event_info['depth_in_km']
        mag = event_info['magnitude']
        mrr = event_info['m_rr'] * 1e7
        mtt = event_info['m_tt'] * 1e7
        mpp = event_info['m_pp'] * 1e7
        mrt = event_info['m_rt'] * 1e7
        mrp = event_info['m_rp'] * 1e7
        mtp = event_info['m_tp'] * 1e7

        # Format.
        return cmt_template.format(
            time_year=start_time.year,
            time_month=start_time.month,
            time_day=start_time.day,
            time_hh=start_time.hour,
            time_mm=start_time.minute,
            time_ss=start_time.second + start_time.microsecond / 1e6,
            event_mag=mag,
            event_name=str(start_time) + '_' + ('%1.f' % mag),
            event_latitude=float(latitude),
            event_longitude=float(longitude),
            event_depth=float(depth),
            half_duration=0.0,
            time_shift=0.0,
            mtt=mtt,
            mrr=mrr,
            mpp=mpp,
            mtp=mtp,
            mrt=mrt,
            mrp=mrp)



    def check_pre_staging(self):
        pass

    def stage_data(self):
        pass

    def check_post_staging(self):
        pass

    def run(self):

        all_events = sorted(self.event_info.keys()) + ['MESH']
        with click.progressbar(
                all_events, label="Generating CMT solutions...") as events:
            for event in events:

                # Just copy some value to the mesh directory (doesn't matter)
                if event == "MESH":
                    event_key = all_events[0]
                else:
                    event_key = event

                # Get relevant parameters.
                event_info = self.event_info[event_key]
                cmt_string = self.generate_cmt_string_for_noise_events(event_info)

                # Write.
                cmt_path = os.path.join(self.config.solver_dir, event, 'DATA',
                                        'CMTSOLUTION')
                self.remote_machine.write_file(cmt_path, cmt_string)


    def check_post_run(self):
        pass
