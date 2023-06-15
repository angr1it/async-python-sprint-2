import time
import logging
import multiprocessing

from .scheduler import Scheduler
from .tasks.getweather import GetWeather
from .tasks.sleeper import Sleeper
from .tasks.foldermaster import FolderMaster
from .tasks.dataanalyze import DataAnalyze
from .tasks.bestjourney import BestJourney

logger = logging.getLogger()
logging.basicConfig(
    handlers=[logging.FileHandler(filename='app.log', mode='w')],
    level=logging.DEBUG,
    format='%(process)d: %(asctime)s: %(levelname)s - %(message)s'
)

def run_n_load():
    scheduler = Scheduler()

    # Структура папок
    folderMaster = FolderMaster(folder_list = [
        'data/raw',
        'data/analyze'
        ])
    
    # Get запросы тут
    weather = GetWeather(result_folder_path = 'data/raw', scheduler=scheduler, dependencies = ['FolderMaster'])
    # Работа с файлами
    analyze = DataAnalyze(data_folder = 'data/raw', result_folder = 'data/analyze', dependencies = ['FolderMaster', 'GetWeather'])
    bestJorney = BestJourney(data_folder='data/analyze', result_folder='data', dependencies=['DataAnalyze'])

    sleeper = Sleeper(epochs = 5, tries = 5)
    
    scheduler.add_tasks([weather, sleeper, folderMaster, analyze, bestJorney])
    proc = multiprocessing.Process(target=scheduler.run, args=())
    proc.start()
    time.sleep(5)
    proc.terminate()

    logger.debug('Main: Scheduler stopped.')
    time.sleep(5)
    logger.debug('Main: Starting from status file.')

    del scheduler

    scheduler = Scheduler(jobs = [])
    scheduler.state.load('status/status.txt')
    
    scheduler.run()


if __name__ == "__main__":
    run_n_load()
