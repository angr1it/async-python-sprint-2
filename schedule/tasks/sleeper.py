import time
import random

from ..job import Job
from ..exceptions import SleeperFailed

class Sleeper(Job):

    def __init__(self, epoch_time = 1, epochs = 1, **kwargs) -> None:
        self.epoch_time = epoch_time
        self.epochs = epochs
        super().__init__(**kwargs)

    def as_dict(self):
        data = {
            'start_at': self.start_at,
            'max_working_time': self.max_working_time,
            'tries': self.tries,
            'dependencies': self.dependencies,
            'status_file': self.status_file,
            'epoch_time': self.epoch_time,
            'epochs': self.epochs
        }

        return data
    
    def _run(self, start_with = 0):
        self.logger.debug('Sleeper: starts.')

        for i in range(start_with, self.epochs):

            # Test retrying
            if random.randint(0, self.tries) > self.tries/2:
                self.logger.error(f'Sleeper: task failed! Returning...')
                raise SleeperFailed()
            
            self.logger.debug(f'Sleeper: sleeps.. {self.epoch_time}s')
            time.sleep(self.epoch_time)

            self._pass_stage(i)
            yield 'none', None

        self.logger.debug('Sleeper: leaves.')
