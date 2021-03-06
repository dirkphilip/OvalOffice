import collections
import time

import click

Job = collections.namedtuple(
    'Job', ['id', 'name', 'status', 'done', 's_time', 'e_time'])


class JobQueue(object):
    """Monitors a collection of jobs on some remote machine.

    This is a custom container class that uses scontrol on the remote SLURM
    machine to gather some relevant information about a submitted job.

    :param system: System object describing the remote cluster.
    :param name: Give your job queue a custom name.
    :type system: System
    :type name: str
    """

    def __init__(self, system, name="Job queue"):

        self.jobs_left = 0
        self.name = name
        self.system = system
        self._jobs = []

    def __len__(self):
        return len(self._jobs)

    def add_job(self, job_number):
        """Add a job to the report queue.

        :param job_number: SLURM job id.
        :type job_number: int
        """

        # Initialize and save to internal job list.
        job = Job(id=job_number,
                  name=None,
                  status=None,
                  s_time=None,
                  e_time=None,
                  done=False)
        job = self._parse_info(job)
        self._jobs.append(job)

        # Reset the number of complete jobs.
        self.jobs_left = 0
        for job in self._jobs:
            if not job.done:
                self.jobs_left += 1

    def _read_so(self, job, so):
        """Read the info from the scontrol stdout
        :param so: stdout from scontrol
        :param job: Named tuple describing the job.
        :type job: Job
        """
        try:
            spl = so.read().split()
            spl = [s for s in spl if "=" in s]
            m = dict()
            for item in spl:
                keyval = item.split("=")
                m[keyval[0]] = keyval[1]
            if m["JobState"] == "COMPLETED":
                return job._replace(status="COMPLETE",
                                    done=True)
        except:
            print "REPORTING ERROR. WAIT UNTIL NEXT INTERVAL."
            return job
        return job._replace(status=m["JobState"],
                            s_time=m["StartTime"],
                            e_time=m["EndTime"],
                            name=m["JobName"])

    def _parse_info(self, job):
        """Query the remote system and update a given job tuple.

        :param job: Named tuple describing the job.
        :type job: Job
        """

        # Do nothing if already marked complete.
        if job.done:
            return job

        _, so, _ = self.system.ssh_connection.exec_command(
            "scontrol show job {}".format(job.id), timeout=10)

        # If scontrol fails, means that job is no longer in queue.
        # Sometimes it incorrectly returns an exit code 1 even though its still in queue on daint
        # try again to be sure..

        if so.channel.recv_exit_status():
            _, so, _ = self.system.ssh_connection.exec_command(
                "scontrol show job {}".format(job.id), timeout=30)

            if so.channel.recv_exit_status():
                return job._replace(status="COMPLETE",
                                    done=True)
            else:
                return self._read_so(job, so)

        # Otherwise, get the information.
        else:
            return self._read_so(job, so)


    def report(self):
        """Get a pretty description of job statuses from the remote system.

        This method will query the remote system (through the internal system
        object), and return a formatted string with the ID, Name, Status,
        Start Time, and End Time, of all contained jobs.

        :returns info_string: Pretty job string, ready to be printed.
        """

        # Initialize and print header.
        info_string = ""
        job_formatter = "{id:<9}{name:<70}{status:<20}" \
                        "{s_time:<20}{e_time:<20}\n"
        info_string += job_formatter.format(id="ID",
                                            name="Name",
                                            status="Status",
                                            s_time="Start Time",
                                            e_time="End Time")

        # Get job info, and reset finished job counts.
        self.jobs_left = 0
        for job in self._jobs:
            job = self._parse_info(job)
            info_string += job_formatter.format(name=job.name,
                                                id=job.id,
                                                status=job.status,
                                                s_time=job.s_time,
                                                e_time=job.e_time)
            if not job.done:
                self.jobs_left += 1

        return info_string

    def flash_report(self, update_interval):
        """Print a formatted report to the screen.

        :param update_interval: Time interval with which to query the system
        :type update_interval: int
        """
        prev_info_string = None
        while self.jobs_left > 0:
            info_string = self.report()
            if info_string != prev_info_string:
                click.clear()
                click.secho(info_string)
                prev_info_string = info_string
            time.sleep(update_interval)
