from pathlib import Path
from typing import List

from ..job import Job

class FolderMaster(Job):

    def __init__(self, folder_list: List[str] = [], **kwargs) -> None:
        self.folder_list = folder_list
        super().__init__(**kwargs)

    def as_dict(self):
        data = {
            'start_at': self.start_at,
            'max_working_time': self.max_working_time,
            'tries': self.tries,
            'dependencies': self.dependencies,
            'status_folder': self.status_folder,
            'folder_list': self.folder_list
        }

        return data
    
    def _run(self, start_with = 0):
        self.logger.debug('FolderMaster: starts.')

        for i in range(start_with, len(self.folder_list)):

            Path(self.folder_list[i]).mkdir(parents=True, exist_ok=True)
            self.logger.debug(f'FolderMaster: creates {self.folder_list[i]}')

            self._pass_stage(i)
            yield 'none', None

        self.logger.debug('FolderMaster: leaves.')
