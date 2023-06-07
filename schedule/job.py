import logging
from typing import Generator

class Job:
    def __init__(self, start_at="", max_working_time=-1, tries=0, dependencies=[]):
        self.logger = logging.getLogger()

    def run(self):
        pass

    def pause(self):
        pass

    def stop(self):
        pass
