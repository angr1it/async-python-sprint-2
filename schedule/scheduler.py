from multiprocessing import Queue
import logging
from typing import Dict

from .tasks.sleeper import Sleeper
from .job import Job

class Scheduler:
    """
    Controls task queue;
    producer;
    """
    def __init__(self, max_priority = 10):

        self.max_priority = max_priority

        self.tasks = []
        for _ in range(max_priority):
            self.tasks.append(Queue())

        self.logger = logging.getLogger()

    def schedule(self, task: Job, priority = 1):
        if priority > self.max_priority:
            raise RuntimeError('Max priority exceeded!')
    
        self.tasks[priority].put(task)

    def pop(self) -> Job:
        for key in range(self.max_priority):
            if not self.tasks[key].empty():
                return self.tasks[key].get()
            
        raise RuntimeError('No task left!')
    
    
    def recv(self, reason, data):
        """
        Receive result of task; 
        decide what to do next; 
        Put another task in the queue according to some logic? Or do nothing?
        TODO: move the conditions into a separate method/class or static method inside particular task
        """

        if reason == 'sleep':
            self.logger.debug(f'Scheduler: Sleeper slept. Data: {data}')
        if reason == 'controller':
            if data == 'sleeper':
                self.schedule(Sleeper(), 3)
            else:
                raise RuntimeError
        return