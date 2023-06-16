import json
import glob

from ..job import Job
from ..utils import write_to_file

class DataAnalyze(Job):

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
            'status_file': self.status_file,
            'data_folder': self.data_folder,
            'result_folder': self.result_folder
        }

        return data
    
    def _run(self, start_with = 0):
        self.logger.debug('DataAnalyze: starts.')

        data_list = glob.glob(f'{self.data_folder}/*.json')

        for i in range(start_with, len(data_list)):
            
            with open(data_list[i], 'r') as file:
                data = json.load(file)
            
            result = dict()
            result['city'] = data['geo_object']['locality']['name']

            result['temp'] = data['fact']['temp']
            result['feels_like'] = data['fact']['feels_like']
            result['wind_speed'] = data['fact']['wind_speed']
            result['is_thunder'] = data['fact']['is_thunder']

            location = f'{self.result_folder}/{result["city"]}.json'

            write_to_file(location, json.dumps(result))

            # with open(location, 'w') as file:
            #     json.dump(result, file)

            self.logger.debug(f'DataAnalyze: result written in {location}')
            self._pass_stage(i)
            yield 'none', None

        self.logger.debug('DataAnalyze: leaves.')
