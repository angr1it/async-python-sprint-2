import unittest
import filecmp
from pathlib import Path
import shutil

from schedule.job import Job
from schedule.scheduler import Scheduler
from schedule.tasks.bestjourney import BestJourney
from schedule.tasks.dataanalyze import DataAnalyze
from schedule.tasks.foldermaster import FolderMaster   

class TestScheduler(unittest.TestCase):

    def tearDown(self) -> None:
        shutil.rmtree('tests/test_results')
    
    def test_run(self):
        
        scheduler = Scheduler()
        
        analyze = DataAnalyze(data_folder = 'tests/test_data/raw', result_folder = 'tests/test_results/analyze', dependencies = [])
        bestJorney = BestJourney(data_folder='tests/test_results/analyze', result_folder='tests/test_results', dependencies=['DataAnalyze'])

        scheduler.add_tasks([analyze, bestJorney])
        scheduler.run()

        self.assertTrue(filecmp.cmp('tests/test_results/analyze/Moscow.json', 'tests/test_data/analyze/Moscow.json'))
        self.assertTrue(filecmp.cmp('tests/test_results/best_candidate.json', 'tests/test_data/best_candidate.json'))

    



if __name__ == '__main__':
    unittest.main()