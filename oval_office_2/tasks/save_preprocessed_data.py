import os

import click

from oval_office_2 import utilities
from . import task


class SavePreprocessedData(task.Task):
    def __init__(self, system, config):
        super(SavePreprocessedData, self).__init__(system, config)
        self.no_data = []
        self.event_info, self.iteration_info = utilities.get_lasif_information(
            self.remote_machine, self.config.lasif_project_path,
            self.config.base_iteration)
        self.all_events = sorted(self.event_info.keys())

    def check_pre_staging(self):

        with click.progressbar(self.all_events, label='Checking for processed data...') as events:
            for event in events:

                event_dir = os.path.join(self.config.preprocessing_dir, event)
                files = self.remote_machine.ftp_connection.listdir(event_dir)
                if 'preprocessed_data.mseed' not in files:
                    self.no_data.append(event)

        for event in self.no_data:
            print 'No data found for: ' + event

    def stage_data(self):
        pass

    def check_post_staging(self):
        pass

    def run(self):

        save_dir = os.path.join(self.config.lasif_project_path, 'DATA')
        with click.progressbar(self.all_events, label='Copying data...') as events:
            for event in events:

                hpass = 1 / self.iteration_info['highpass']
                lpass = 1 / self.iteration_info['lowpass']
                event_dir = os.path.join(save_dir, event, 'preprocessed_{:.1f}_{:.1f}'.format(lpass, hpass))
                self.remote_machine.makedir(event_dir)

    def check_post_run(self):
        pass
