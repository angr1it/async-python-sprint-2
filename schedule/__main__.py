

import logging

from .scheduler import Scheduler
from .tasks.getweather import GetWeather
from .tasks.cleaner import Cleaner
from .tasks.sleeper import Sleeper
from .tasks.foldermaster import FolderMaster
from .tasks.dataanalyze import DataAnalyze
from .tasks.bestjourney import BestJourney
from .state import SchedulerState

logger = logging.getLogger()
logging.basicConfig(
    handlers=[logging.FileHandler(filename='app.log', mode='w')],
    level=logging.DEBUG,
    format='%(process)d: %(asctime)s: %(levelname)s - %(message)s'
)

def run_new():
    scheduler = Scheduler()

    folderMaster = FolderMaster(folder_list = [
        'data/raw',
        'data/analyze'
        ])
    
    weather = GetWeather(result_folder_path = 'data/raw', scheduler=scheduler, dependencies = ['FolderMaster'])
    analyze = DataAnalyze(data_folder = 'data/raw', result_folder = 'data/analyze', dependencies = ['FolderMaster', 'GetWeather'])
    best_jorney = BestJourney(data_folder='data/analyze', result_folder='data', dependencies=['DataAnalyze'])
    sleeper = Sleeper(epochs = 5)

    scheduler.add_tasks([weather, sleeper, folderMaster, analyze, best_jorney])
    scheduler.run()

def run_from_load():
    scheduler = Scheduler()
    scheduler.state.load('status/status.txt')
    
    scheduler.run()

if __name__ == "__main__":
    #run_new()
    run_from_load()