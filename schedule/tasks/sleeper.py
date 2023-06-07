import time

from ..job import Job


class Sleeper(Job):
    def __init__(self, s = 1, start_at="", max_working_time=-1, tries=0, dependencies=[]):
        super().__init__()
        self.s = s

    def run(self):
        self.logger.debug('Sleeper: sleeps..')
        time.sleep(self.s)
        yield ('sleep', self.s)
        self.logger.debug('Sleeper: exits.')
