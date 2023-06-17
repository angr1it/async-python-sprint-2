import logging
import json
import importlib
from collections import deque
from typing import List, Generator, Tuple

from .job import Job
from .utils import write_to_file


class SchedulerState:
    def __init__(self, waiting: List[Job] = None, scheduler = None, status_folder = 'status') -> None:
        if waiting is None:
            waiting = []
        
        self.logger = logging.getLogger()
        self.scheduler = scheduler

        self.status_folder = status_folder
        self.task_folder = f'{status_folder}/tasks'

        self.waiting = waiting
        self.passed = []
        self.running = []
        
        self.tasks_to_run = deque([])

    def add_to_waiting(self, jobs: List[Job]) -> None:
        for job in jobs:
            self.waiting.append(job)

        self.logger.debug(f'SchedulerState: Jobs {jobs} added to wait list.')
        self.dump()

    def load(self, filepath: str = None) -> None:
        if filepath is None:
            # из-за self.
            filepath = f'{self.status_folder}/status.txt'
        
        with open(filepath, 'r') as file:
            data = json.load(file)

        for name, id in data['waiting']:
            self.waiting.append(self.__inst_job(name, id))

        for name, id in data['running']:
            job = self.__inst_job(name, id)
            self.running.append(job)
            self.tasks_to_run.append((
                job,
                # TODO: write status-{id}.txt path inside status.txt
                job.run_on_load(f'{self.task_folder}/status-{id}.txt')
            ))

        for name, id in data['passed']:
            self.passed.append(self.__inst_job(name, id))

        self.logger.debug(f'SchedulerState: state loaded from {self.status_folder}/status.txt')
            
    def __inst_job(self, class_name, id) -> Job:

        module = importlib.import_module(f'schedule.tasks.{class_name.lower()}')
        class_ = getattr(module, class_name)

        with open(f'{self.task_folder}/init-{id}.json', 'r') as file:
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

        self.logger.debug(f'SchedulerState: Job created from {self.task_folder}/init-{id}.json')
        return job

    def dump(self, filepath: str = None) -> None:
        if filepath is None:
            filepath = f'{self.status_folder}/status.txt'

        data = {
            'waiting': [ (type(item).__name__, id(item)) for item in self.waiting],
            'passed': [ (type(item).__name__, id(item)) for item in self.passed],
            'running': [ (type(item).__name__, id(item)) for item in self.running]
        }

        write_to_file(filepath, json.dumps(data, indent=4))

        self.logger.debug(f'SchedulerState: File {self.status_folder}/status.txt updated')

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
    
    def job_in_task_runnung(self, job: Job) -> bool:
        # TODO: maybe use hash or something
        for item in self.tasks_to_run:
            if job == item[0]:
                return True
            
        return False

    def continue_task(self, task: Tuple[Job, Generator]):

        if self.job_in_task_runnung(task[0]):
            raise RuntimeError(f'Job {task[0]} is already included in tasks running!')
        
        self.tasks_to_run.append(task)

    def __remove_from_tasks_to_run(self, job: Job) -> bool:
        # TODO: maybe use hash or something
        for item in self.tasks_to_run:
            if job == item[1]:
                self.tasks_to_run.remove(item)
                return True
            
        return False
    
    def return_to_wait_job(self, job):
        
        if job in self.passed:
            raise RuntimeError('Cannot return passed job!')
        
        if job in self.waiting:
            self.logger.error(f'SchedulerState: return_to_wait_job -- job {job} is already in waiting!')
            return
        
        if job in self.running:
            self.running.remove(job)
        
        self.__remove_from_tasks_to_run(job)

        self.waiting.append(job)

    def pass_task(self, job: Job):
        if not job in self.running:
            return RuntimeError()
        
        self.running.remove(job)
        self.passed.append(job)

        self.__remove_from_tasks_to_run(job)
        
        self.logger.debug(f'SchedulerState: job {job} passed')
        self.dump()