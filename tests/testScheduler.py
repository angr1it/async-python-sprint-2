import unittest
import filecmp
import shutil

from schedule.scheduler import Scheduler
from schedule.tasks.bestjourney import BestJourney
from schedule.tasks.dataanalyze import DataAnalyze 
from schedule.tasks.sleeper import Sleeper
from schedule.tasks.breaker import Breaker


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

    def test_run_and_load(self):
        scheduler = Scheduler()
        
        analyze = DataAnalyze(data_folder = 'tests/test_data/raw', result_folder = 'tests/test_results/analyze', dependencies = [])
        bestJorney = BestJourney(data_folder='tests/test_results/analyze', result_folder='tests/test_results', dependencies=['DataAnalyze', 'Sleeper', 'Breaker'])
        sleeper = Sleeper(1, 3)
        breaker = Breaker(dependencies = ['Sleeper'])

        scheduler.add_tasks([analyze, bestJorney, sleeper, breaker])
        scheduler.run()

        del scheduler

        scheduler = Scheduler()
        scheduler.state.load('status/status.txt')
        
        scheduler.run()

        self.assertTrue(filecmp.cmp('tests/test_results/analyze/Moscow.json', 'tests/test_data/analyze/Moscow.json'))
        self.assertTrue(filecmp.cmp('tests/test_results/best_candidate.json', 'tests/test_data/best_candidate.json'))

if __name__ == '__main__':
    unittest.main()