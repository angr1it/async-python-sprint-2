from multiprocessing import Queue
import logging

from .tasks.sleeper import Sleeper
from .job import Job

class Scheduler:
    """
    Controls task queue;
    producer;
    """
    def __init__(self):
        self.task_queue = Queue()
        self.logger = logging.getLogger()

    def schedule(self, task: Job):
        self.task_queue.put(task)

    def pop(self) -> Job:
        return self.task_queue.get()
    
    def recv(self, reason, data):
        """
        Receive result of task; 
        decide what to do next; 
        Put another task in the queue according to some logic? Or do nothing?
        """

        if reason == 'sleep':
            self.logger.debug(f'Scheduler: Sleeper slept. Data: {data}')
        if reason == 'controller':
            if data == 'sleeper':
                self.task_queue.put(Sleeper())
            else:
                raise RuntimeError
        return