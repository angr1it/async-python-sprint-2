import logging
from multiprocessing import Process

from .scheduler import Scheduler

class Worker(Process):
    """
    Realisation of event loop;
    consumer;
    can start multiple threads;
    """
    def __init__(self, scheduler: Scheduler, pool_size=10):
        super().__init__(daemon=True)
        self.scheduler = scheduler
        self.logger = logging.getLogger()

    def run(self):
        self.logger.debug('Worker: starts.')
        while not self.scheduler.task_queue.empty():
            job = self.scheduler.pop()
            task = job.run()

            while True:
                try:
                    reason, data = next(task)
                    self.scheduler.recv(reason, data)
                except StopIteration:
                    self.logger.debug(f'Worker: Task {type(task)} finished through StopIteration.')
                    break    
                except Exception as ex:
                    self.logger.debug(f'Worker: Task {type(task)} finished because of: {ex}')
                    break

        self.logger.debug('Worker: exits.')


    def restart(self):
        pass

    def stop(self):
        pass