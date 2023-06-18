import unittest
import shutil
import json
from typing import *
import os
import filecmp

from schedule.tasks.bestjourney import BestJourney
from schedule.tasks.dataanalyze import DataAnalyze
from schedule.tasks.foldermaster import FolderMaster   
from schedule.tasks.sleeper import Sleeper
from schedule.tasks.breaker import Breaker
from schedule.job import Job

from schedule.state import SchedulerState


class TestTasks(unittest.TestCase):

    def tearDown(self) -> None:
        shutil.rmtree('tests/test_results')
        pass
    

    def test_analyze(self):

        job = DataAnalyze('tests/test_data/raw', 'tests/test_results', status_folder = 'tests/test_results/state')

        task = job.run()

        self.assertEqual(next(task), ('none', None))
        self.assertEqual(next(task), ('none', None))
        with self.assertRaises(StopIteration):
            next(task)

        with open('tests/test_results/London.json', 'r') as file:
            self.assertEqual(file.read(), '{"city": "London", "temp": 15, "feels_like": 13, "wind_speed": 4.1, "is_thunder": false}')

        with open('tests/test_results/Moscow.json', 'r') as file:
            self.assertEqual(file.read(), '{"city": "Moscow", "temp": 19, "feels_like": 18, "wind_speed": 1.7, "is_thunder": false}')

        self.assertTrue(filecmp.cmp(f'tests/test_results/state/init-{id(job)}.json', 'tests/test_data/state/tasks/init.json'))
        self.assertTrue(filecmp.cmp(f'tests/test_results/state/status-{id(job)}.txt', 'tests/test_data/state/tasks/status.txt'))