import json
import glob
from operator import itemgetter

from ..job import Job
from ..utils import write_to_file

class BestJourney(Job):

    def __init__(self, data_folder = 'data', result_folder = 'data', **kwargs) -> None:
        self.data_folder = data_folder
        self.result_folder = result_folder
        super().__init__(**kwargs)

    def as_dict(self):
        data = {
            'start_at': self.start_at,
            'max_working_time': self.max_working_time,
            'tries': self.tries,
            'dependencies': self.dependencies,
            'status_folder': self.status_folder,
            'data_folder': self.data_folder,
            'result_folder': self.result_folder
        }

        return data
    
    def _run(self, start_with = 0):
        self.logger.debug('BestJourney: starts.')

        path_list = glob.glob(f'{self.data_folder}/*.json')

        data = []
        for path in path_list:
            with open(path, 'r') as file:
                data.append(json.load(file))
        
        data = sorted(data, key=lambda d: int(d['is_thunder'] == True) )
        data = sorted(data, key=itemgetter('wind_speed'))
        data = sorted(data, key=itemgetter('feels_like'), reverse=True)

        write_to_file(f'{self.result_folder}/best_candidate.json', json.dumps({'best_candidate': data[0]}))


        self.logger.info(f'BestJourney: Best candidate found. Result written into {self.result_folder}/best_candidate.json')

        yield 'none', None

        self.logger.debug('BestJourney: leaves.')
