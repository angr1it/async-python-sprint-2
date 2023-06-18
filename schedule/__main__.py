import time
import logging

from .scheduler import Scheduler
from .tasks.getweather import GetWeather
from .tasks.sleeper import Sleeper
from .tasks.foldermaster import FolderMaster
from .tasks.dataanalyze import DataAnalyze
from .tasks.bestjourney import BestJourney
from .tasks.breaker import Breaker

# Касательно TypeError: cannot pickle 'select.kqueue' object -- не поймал ошибку ни у себя (в .devcontainer указан docker образ), ни в тестах.
# Подозреваю, что возникает при работе с multiprocessing, что не является необходимым для данного решения. В предыдущей версии использовал только 
# для того, чтобы показать прерывание выполнения sheduler и загрузки состояния из файлов -- добавил для этой цели Breaker;


logger = logging.getLogger()
logging.basicConfig(
    handlers=[logging.FileHandler(filename='app.log', mode='w')],
    level=logging.DEBUG,
    format='%(process)d: %(asctime)s: %(levelname)s - %(message)s'
)

def run_n_load():
    scheduler = Scheduler()

    folderMaster = FolderMaster(folder_list = [
        'data/raw',
        'data/analyze'
        ])
    
    weather = GetWeather(result_folder_path = 'data/raw', scheduler=scheduler, dependencies = ['FolderMaster'])
    analyze = DataAnalyze(data_folder = 'data/raw', result_folder = 'data/analyze', dependencies = ['FolderMaster', 'GetWeather', 'Breaker'])
    bestJorney = BestJourney(data_folder='data/analyze', result_folder='data', dependencies=['DataAnalyze'])

    breaker = Breaker(dependencies = ['FolderMaster'])

    sleeper = Sleeper(epochs = 5, tries = 5)
    
    scheduler.add_tasks([weather, sleeper, folderMaster, analyze, bestJorney, breaker])
    scheduler.run()

    logger.debug('Main: Scheduler stopped.')
    time.sleep(5)
    logger.debug('Main: Starting from status file.')

    del scheduler

    scheduler = Scheduler(jobs = [])
    scheduler.state.load('status/status.txt')
    
    scheduler.run()


if __name__ == "__main__":
    run_n_load()
