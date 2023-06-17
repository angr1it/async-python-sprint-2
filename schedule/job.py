import os
import logging
import json
from datetime import datetime, timedelta
from typing import List

from .exceptions import (
    TimeOutGenerator,
    WaitForStart,
    TaskCompleted
)
from .utils import write_to_file


class Job(object):
    def __init__(self, start_at='', max_working_time =-1, tries=0, dependencies: List[str] = None, status_folder = 'status/tasks') -> None:
        if dependencies is None:
            dependencies = []
        
        self.logger = logging.getLogger()
        self._init_new(start_at, max_working_time, tries, dependencies, status_folder)
    
    def _init_new(self, start_at, max_working_time, tries, dependencies, status_folder):
        self.time_start = datetime.now()

        self.max_working_time = max_working_time
        self.time_end = datetime.now() + timedelta(0, max_working_time)
        
        if not start_at:
            self.start_at = None
        else:
            self.start_at = None if start_at == '' else datetime.strptime(start_at, '%m/%d/%y %H:%M:%S')

        self.tries = tries

        self.dependencies = dependencies
        
        self.status_folder = status_folder
        self.stage_state_file = f'{status_folder}/status-{id(self)}.txt'
        self.init_state_file = f'{status_folder}/init-{id(self)}.json'

        write_to_file(self.stage_state_file, "{'status': 0, 'stage': 0}\n")

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
            'status_folder': self.status_folder
        }

        return data

    def dump_init_data(self):
        
        data = self.as_dict()

        write_to_file(self.init_state_file, json.dumps(data))

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
        
        if datetime.now() >  self.time_end and self.max_working_time > -1:
            raise TimeOutGenerator
        
        if not self.ready():
            raise WaitForStart

    def _pass_stage(self, stage):
        status = {
            'status': 1,
            'stage': stage
        }

        # write_to_file(self.status_file, json.dumps(status) + '\n')  -- path check already done via init
        with open(self.stage_state_file, 'a') as f:
            f.write(json.dumps(status) + '\n')

    def _end(self):
        with open(self.stage_state_file, 'a') as f:
            f.write("{'status': 2, 'stage': -1}")
    
    def run(self, start_with = 0):
        yield from self._run(start_with)

        self._end()

    def _run(self, **kwargs):
        pass

    def ready(self) -> bool:

        if self.start_at:
            if datetime.now() < self.start_at:
                return False
        
        return True

    def run_on_load(self, status_filepath):

        stage = Job.read_stage(status_filepath)
        if (stage == -1):
            raise TaskCompleted()
        
        return self.run(stage)