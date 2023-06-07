import time

from ..job import Job


class Controller(Job):
    def __init__(self, start_at="", max_working_time=-1, tries=0, dependencies=[]):
        super().__init__()

    def run(self):
        self.logger.debug('Controller: takes job.')
        time.sleep(1)
        self.logger.debug('Controller: forwarded.')
        yield ('controller', 'sleeper')
        self.logger.debug('Controller: exits.')
