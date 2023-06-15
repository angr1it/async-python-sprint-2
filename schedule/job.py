import os
import logging
import json
from typing import Any, Generator
from datetime import datetime, timedelta

from .exceptions import (
    TimeOutGenerator,
    WaitForStart,
    TaskCompleted
)

TASK_FOLDER = 'status/tasks'

class Job(object):
    def __init__(self, start_at='', max_working_time =-1, tries=0, dependencies: list[str] = [], status_file = '{}/status-{}.txt') -> None:
        """_summary_

        Args:
            start_at (str, optional): datetime string in format '%m/%d/%y %H:%M:%S'. Defaults to '' -- start without delay.
            max_working_time (int, optional): _description_. Defaults to -1.
            tries (int, optional): _description_. Defaults to 0.
            dependencies (list[str], optional): List of __name__s of jobs that has to pass before current job. Defaults to [].
            status_file (list[str], optional): __. Defaults to 'status-{}.txt'.
        """
        self.logger = logging.getLogger()
        self._init_new(start_at, max_working_time, tries, dependencies, status_file)
    
    def _init_new(self, start_at, max_working_time, tries, dependencies, status_file):
        self.time_start = datetime.now()

        self.max_working_time = max_working_time
        self.time_end = datetime.now() + timedelta(0, max_working_time)
        
        if not start_at:
            self.start_at = None
        else:
            self.start_at = None if start_at == '' else datetime.strptime(start_at, '%m/%d/%y %H:%M:%S')

        self.tries = tries

        self.dependencies = dependencies
        self.status_file = status_file.format(TASK_FOLDER, id(self))

        with open(self.status_file, 'w') as f:
            f.write("{'status': 0, 'stage': 0}\n")

        self.dump_init_data()
    
    def get_secs_before_start(self) -> float:

        if not self.start_at:
            return 0
        
        delta = (self.start_at - datetime.now()).total_seconds()

        if delta < 0:
            return 0
        
        return delta

    def as_dict(self):
        data = {
            'start_at': self.start_at,
            'max_working_time': self.max_working_time,
            'tries': self.tries,
            'dependencies': self.dependencies,
            'status_file': self.status_file
        }

        return data

    def dump_init_data(self):
        
        data = self.as_dict()

        with open(f'{TASK_FOLDER}/init-{id(self)}.json', 'w') as outfile:
            json.dump(data, outfile)

    def load_init_data(self, filepath):
        """assumed that data already created by the class at the previous iteration is being loaded -- otherwise will lead to an error somewhere

        Args:
            filepath (_type_): _description_
        """

        data = json.load(filepath)

        for k, v in data.items():
            setattr(self, k, v)

        self.dump_init_data()

    @staticmethod
    def read_stage(filepath):
        data = Job._read_stage_status(filepath)
        return data['stage']
    
    @staticmethod
    def _read_stage_status(filepath) -> dict:
        # TODO: rewrite probably
        with open(filepath, 'rb') as f:
            try:
                f.seek(-2, os.SEEK_END)
                while f.read(1) != b'\n':
                    f.seek(-2, os.SEEK_CUR)
            except OSError:
                f.seek(0)
            last_line = f.readline().decode()

        return json.loads(last_line.replace("\'", "\""))
    
    def resolve_meta(self):
        # TODO: rewrite probably
        if datetime.now() >  self.time_end and self.max_working_time > -1:
            raise TimeOutGenerator
        
        if not self.ready():
            raise WaitForStart

    def _pass_stage(self, stage):
        status = {
            'status': 1,
            'stage': stage
        }
        with open(self.status_file, 'a') as f:
            f.write(status.__str__() + '\n')

    def _end(self):
        with open(self.status_file, 'a') as f:
            f.write("{'status': 2, 'stage': -1}")
    
    def run(self, start_with = 0):
        yield from self._run(start_with)

        self._end()

    def _run(self, **kwargs):
        pass

    def ready(self) -> bool:
        # TODO: rewrite probably
        if self.start_at:
            if datetime.now() < self.start_at:
                return False
        
        return True

    def get_meta(self):
        # TODO: rewrite probably
        pass

    def pause(self):
        # TODO: rewrite probably
        pass

    def stop(self):
        # TODO: rewrite probably
        pass

    def run_on_load(self, status_filepath):
        """Runs with paramas on load to continue;
        """
        stage = Job.read_stage(status_filepath)
        if (stage == -1):
            raise TaskCompleted()
        
        return self.run(stage)