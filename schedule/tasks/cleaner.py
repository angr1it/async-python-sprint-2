import glob
import os

from ..job import Job


class Cleaner(Job):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def run(self):
        links = []
        links.extend(glob.glob('status/tasks/status-*.txt'))
        links.extend(glob.glob('status/tasks/init-*.json'))
        for link in links:
            os.remove(link)
            self.logger.debug(f'Status file {link} removed.')