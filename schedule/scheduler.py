import sys
# Добавлено в качестве попытки решить ошибку (при работе с selectors) с WSAStartup при тестировании на windows: OSError: [WinError 10093]
# что не решило проблему ==> OSError: [WinError 10022]. Пока отключаю тесты на win, т.к. нет времени искать другое решение.
if sys.platform.startswith('win'):
    import win_inet_pton

import time
import logging
import selectors
from typing import List, Tuple, Generator

from .state import SchedulerState
from .tasks.cleaner import Cleaner
from .job import Job
from .exceptions import (
    TimeOutGenerator,
    WaitForStart,
    NoJobsLeftWaiting,
    ContinueLoop,
    ExitLoopWithWaitingJobsLeft,
    ExitLoopTaskCommand
)


class Scheduler:
    def __init__(self, jobs: List[Job] = None):
        if jobs is None:
            jobs = []

        self.logger = logging.getLogger()

        self.selector = selectors.DefaultSelector()

        self.state = SchedulerState(jobs, scheduler=self)
    
    def add_tasks(self, jobs: List[Job]) -> None:
        self.state.add_to_waiting(jobs)

    def try_start_all(self):

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
        
    def on_task_pass(self, job, start_all: bool = True, dump: bool = True):

        self.state.pass_task(job)

        if start_all:
            self.try_start_all()

        if dump:
            self.state.dump()

    def try_start_task(self, job: Job):

        if not job in self.state.waiting:
            raise RuntimeError('Starting a job that not found in waiting.')
        
        if not self.__job_dependencies__ready(job):
            self.logger.debug(f'Job {self} dependencies not done.')
            return False
        
        if not job.ready():
            self.logger.debug(f'Job {self} not ready to start.')
            return False

        return self.state.start_task(job)
    
    def continue_task(self, task: Tuple[Job, Generator]):
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

    def try_retry_job(self, job: Job) -> None:

        if job.tries > 0:
            job.tries -= 1
            self.state.return_to_wait_job(job)
            self.logger.info(f'Retrying job {job}... Tries left: {job.tries}')
        else:
            self.logger.error(f'Job {job} has no tries left. Abandoning.')

    def run(self):
        self.logger.debug('Scheduler: Loop starts.')

        self.try_start_all()

        clean_exit = False

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
                    self.try_retry_job(obj)
                    continue
                except WaitForStart:
                    # TODO: Obsolete?
                    self.logger.debug(f'Scheduler: Task {task} waiting for start.')
                    self.continue_task(item)
                    continue

                try:
                    operation, arg = next(task)
                except StopIteration:
                    self.logger.debug(f'Scheduler: Task {task} passed.')
                    self.on_task_pass(obj)
                    continue
                except ExitLoopTaskCommand:
                    self.logger.debug(f'Scheduler: Task {task} ordered to break the loop. Passing current job and exiting..')
                    self.on_task_pass(obj, start_all=False)
                    break
                except Exception as ex:
                    self.logger.debug(f'Scheduler: Task {task} stopped due: {ex}')
                    self.try_retry_job(obj)
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
                        clean_exit = True
                        break
                    except ExitLoopWithWaitingJobsLeft:
                        self.logger.error(f'Scheduler: jobs left waiting {self.state.waiting} could not be run!')
                        clean_exit = True
                        break

                    time.sleep(sleep_time)
        
        if clean_exit:
            Cleaner().run()
        self.logger.debug('Scheduler: Loop ends.')