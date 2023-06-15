import time
import logging
from typing import *
from collections import deque
import selectors
from datetime import datetime, timedelta

from .state import SchedulerState
from .tasks.cleaner import Cleaner
from .job import Job
from .exceptions import (
    TimeOutGenerator,
    WaitForStart,
    NoJobsLeftWaiting,
    ContinueLoop,
    ExitLoopWithWaitingJobsLeft
)



class Scheduler:
    def __init__(self, jobs: list[Job] = []):
        self.logger = logging.getLogger()

        self.selector = selectors.DefaultSelector()

        self.state = SchedulerState(jobs, scheduler=self)
    
    def add_tasks(self, jobs: list[Job]) -> None:
        self.state.add_to_waiting(jobs)

    def try_start_all(self):
        #iterate through copy
        for job in self.state.waiting[:]:
            self.try_start_task(job)

    def __job_dependencies__ready(self, job: Job) -> bool:
        """Function checks two conditions:
        1. Checks that for each job dependency there is an object in self.state.passed
        2. Checks that there is no job in state.waiting or state.running with a class name from dependencies

        Args:
            job (Job): _description_

        Returns:
            bool: True if both conditions are true; otherwise False
        """

        for name in job.dependencies:
            passed = False

            for job in self.state.passed:
                if job.__class__.__name__ == name:
                    passed = True
                    break

            if not passed:
                return False
        
        for name in job.dependencies:

            for job in self.state.running:
                if job.__class__.__name__ == name:
                    return False

            for job in self.state.waiting:
                if job.__class__.__name__ == name:
                    return False
        
        return True
        
    def on_task_pass(self, job):

        self.state.pass_task(job)

        self.try_start_all()

        self.state.dump()

    def try_start_task(self, job: Job):

        if not job in self.state.waiting:
            raise RuntimeError('Starting a job that not found in waiting.')
        
        if not self.__job_dependencies__ready(job):
            return False
        
        if not job.ready():
            return False

        return self.state.start_task(job)

    def on_update(self):
        # TODO: Things to do before event loop ends: returns True
        return False
    
    def continue_task(self, task: tuple[Job, Generator]):
        self.state.continue_task(task)
    
    def sock_recv(self, sock, n):
        yield 'wait_read', sock
        return sock.recv(n)

    def sock_sendall(self, sock, data):
        yield 'wait_write', sock
        sock.sendall(data)

    def sock_accept(self, sock):
        yield 'wait_read', sock
        return sock.accept()
    
    def try_wait_before_exit(self) -> float:
        
        self.try_start_all()
        if self.state.tasks_to_run:
            raise ContinueLoop()

        if len(self.state.waiting) < 1:
            raise NoJobsLeftWaiting() 
        
        max_wait_time = 0
        for job in self.state.waiting:
            max_wait_time = max(max_wait_time, job.get_secs_before_start())

        if max_wait_time == 0:
            raise ExitLoopWithWaitingJobsLeft()
        
        return max_wait_time

    def run(self):
        self.logger.debug('Scheduler: Loop starts.')

        self.try_start_all()

        while True:
            for key, _ in self.selector.select(timeout=1):
                    data = key.data
                    fileobj = key.fileobj
                    self.selector.unregister(fileobj)
                    self.continue_task(data)
                    
            if self.state.tasks_to_run:
                item = self.state.tasks_to_run.popleft()
                (obj, task) = item
                
                try:
                    if obj:
                        obj.resolve_meta()
                except TimeOutGenerator:
                    self.logger.debug(f'Scheduler: Task {task} timeout.')
                    # TODO: to passed? TO waiting if retry
                    continue
                except WaitForStart:
                    # TODO: Obsolete?
                    self.logger.debug(f'Scheduler: Task {task} waiting for start.')
                    self.continue_task(item)
                    continue

                try:
                    operation, arg = next(task)
                except StopIteration:
                    # TODO: change in status file; job ended -- shoudn't be restarted
                    self.logger.debug(f'Scheduler: Task {task} stops.')
                    self.on_task_pass(obj)
                    continue

                self.logger.debug(f'Scheduler: Task {task} operation: {operation}')
                if operation == 'wait_read':
                    self.selector.register(arg, selectors.EVENT_READ, item)
                elif operation == 'wait_write':
                    self.selector.register(arg, selectors.EVENT_WRITE, item)
                elif operation == 'none':
                    self.continue_task(item)
                else:
                    raise ValueError('Unknown event loop operation:', operation)
            else:
                if not self.state.tasks_to_run:
                    try:
                        sleep_time = self.try_wait_before_exit()
                    except ContinueLoop:
                        continue
                    except NoJobsLeftWaiting:
                        self.logger.debug('Scheduler: There is nothing to do left.')
                        break
                    except ExitLoopWithWaitingJobsLeft:
                        self.logger.error(f'Scheduler: jobs left waiting {self.state.waiting} could not be run!')
                        break

                    time.sleep(sleep_time) # TODO: Maybe add sleeper task
                    # TODO: What if at this point there is something left in self.selector?
        
        #Cleaner().run()
        self.logger.debug('Scheduler: Loop ends.')