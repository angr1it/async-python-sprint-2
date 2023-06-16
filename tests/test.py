import unittest
import filecmp


from schedule.job import Job
from schedule.scheduler import Scheduler
from schedule.tasks.bestjourney import BestJourney
from schedule.tasks.dataanalyze import DataAnalyze
from schedule.tasks.foldermaster import FolderMaster   

class TestScheduler(unittest.TestCase):
    def test_run(self):
        
        scheduler = Scheduler()
        
        folderMaster = FolderMaster(folder_list = [
            'tests/data/raw',
            'tests/data/analyze'
            ])
        
        analyze = DataAnalyze(data_folder = 'tests/data/raw', result_folder = 'tests/data/analyze', dependencies = ['FolderMaster'])
        bestJorney = BestJourney(data_folder='tests/data/analyze', result_folder='tests/data', dependencies=['DataAnalyze'])

        scheduler.add_tasks([folderMaster, analyze, bestJorney])
        scheduler.run()

        self.assertTrue(filecmp.cmp('tests/data/analyze_sample/Moscow.json', 'tests/data/analyze/Moscow.json'))



if __name__ == '__main__':
    unittest.main()