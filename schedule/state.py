import logging
import json
import importlib
from collections import deque
from typing import *

from .job import Job

FOLDER = 'status'
TASK_FOLDER = 'status/tasks'

class SchedulerState:
    def __init__(self, waiting: list[Job] = [], scheduler = None) -> None:
        self.logger = logging.getLogger()
        self.scheduler = scheduler

        self.waiting = waiting
        self.passed = []
        self.running = []
        
        self.tasks_to_run = deque([])

    def add_scheduler(self, scheduler):
        """Need reference after load from status-file.
        TODO: find a better way

        Args:
            scheduler (_type_): _description_
        """
        self.scheduler = scheduler
    
    def add_to_waiting(self, jobs: list[Job]) -> None:
        for job in jobs:
            self.waiting.append(job)

        self.logger.debug(f'SchedulerState: Jobs {jobs} added to wait list.')
        self.dump()

    def load(self, filepath = f'{FOLDER}/status.txt') -> None:
        with open(filepath, 'r') as file:
            data = json.load(file)

        for name, id in data['waiting']:
            self.waiting.append(self.__inst_job(name, id))

        for name, id in data['running']:
            job = self.__inst_job(name, id)
            self.running.append(job)
            self.tasks_to_run.append((
                job,
                job.run_on_load(f'{TASK_FOLDER}/status-{id}.txt')
            ))

        for name, id in data['passed']:
            self.passed.append(self.__inst_job(name, id))

        self.logger.debug(f'SchedulerState: state loaded from {FOLDER}/status.txt')
            
    def __inst_job(self, class_name, id) -> Job:

        module = importlib.import_module(f'schedule.tasks.{class_name.lower()}')
        class_ = getattr(module, class_name)

        with open(f'{TASK_FOLDER}/init-{id}.json', 'r') as file:
            args = json.load(file)
        
        if 'get_scheduler' in args:
            if args['get_scheduler']:
                args['scheduler'] = self.scheduler
                job = class_(**args)
            else:
                raise RuntimeError()
        else:
            job = class_(**args)

        if not isinstance(job, Job):
            raise RuntimeError
        
        job.dump_init_data()

        self.logger.debug(f'SchedulerState: Job created from {TASK_FOLDER}/init-{id}.json')
        return job

    def dump(self, filepath = f'{FOLDER}/status.txt') -> None:
        data = {
            'waiting': [ (type(item).__name__, id(item)) for item in self.waiting],
            'passed': [ (type(item).__name__, id(item)) for item in self.passed],
            'running': [ (type(item).__name__, id(item)) for item in self.running]
        }

        with open(filepath, 'w') as file:
            json.dump(data, file)

        self.logger.debug(f'SchedulerState: File {FOLDER}/status.txt updated')

    def start_task(self, job: Job):

        if not job in self.waiting:
            raise RuntimeError()
        
        self.waiting.remove(job)
        self.running.append(job)
        
        # TODO: add params
        task = job.run()

        self.tasks_to_run.append((job, task))

        self.dump()

        self.logger.debug(f'SchedulerState: Job {job} spawned task {task}')
        return True
    
    def continue_task(self, task: tuple[Job, Generator]):
        self.tasks_to_run.append(task)

    def pass_task(self, job: Job):
        if not job in self.running:
            return RuntimeError()
        
        # 1. Transfer current task to passed;
        self.running.remove(job)
        self.passed.append(job)

        for item in self.tasks_to_run:
            if job == item[1]:
                self.tasks_to_run.remove(item)
                break
        
        self.logger.debug(f'SchedulerState: job {job} passed')
        self.dump()