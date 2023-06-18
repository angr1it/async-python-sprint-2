import unittest
import shutil
import json
from typing import *

from schedule.tasks.bestjourney import BestJourney
from schedule.tasks.dataanalyze import DataAnalyze
from schedule.tasks.foldermaster import FolderMaster   
from schedule.tasks.sleeper import Sleeper
from schedule.tasks.breaker import Breaker
from schedule.job import Job

from schedule.state import SchedulerState

class TestScheduler(unittest.TestCase):

    def tearDown(self) -> None:
        #shutil.rmtree('tests/test_results')
        pass
    
    def __assert_dump_dict(self, result: dict, expected: dict, stage: str = 'waiting') -> None:
        result_list = result[stage]
        expect_list = expected[stage]

        self.assertEqual(len(result_list), len(expect_list))

        result_jobs = [pair[0] for pair in result_list]
        expect_jobs = [pair[0] for pair in expect_list]

        self.assertSetEqual(frozenset(result_jobs), frozenset(expect_jobs))

    def test_state_expected(self) -> None:
        sleeper = Sleeper(status_folder = 'tests/test_results/tasks')
        breaker = Breaker(status_folder = 'tests/test_results/tasks')
        bj = BestJourney(status_folder = 'tests/test_results/tasks')
        da = DataAnalyze(status_folder = 'tests/test_results/tasks')

        state = SchedulerState(waiting=[sleeper, bj, da, breaker], scheduler=None, status_folder='tests/test_results')
        state.start_task(breaker)
        state.start_task(sleeper)
        state.pass_task(breaker)

        state.dump()

        with open('tests/test_results/status.txt', 'r') as file:
            dict1 = json.load(file)

        with open('tests/test_data/state/status.txt', 'r') as file:
            dict2 = json.load(file)

        self.__assert_dump_dict(dict1, dict2, 'waiting')
    
    def __get_job_names(self, jobs: List[Job]) -> List[str]:
        return [job.__class__.__name__ for job in jobs]
    
    def test_state_load(self):

        state = SchedulerState(status_folder='tests/test_results')

        fm = FolderMaster(status_folder = 'tests/test_results/tasks')
        sleeper = Sleeper(status_folder = 'tests/test_results/tasks')
        breaker = Breaker(status_folder = 'tests/test_results/tasks')

        state.waiting = [sleeper, fm]
        state.passed = [breaker]
        state.start_task(fm)

        state.dump()

        state2 = SchedulerState(status_folder='tests/test_results')
        state2.load('tests/test_results/status.txt')

        self.assertListEqual(self.__get_job_names(state.waiting), self.__get_job_names(state2.waiting))
        self.assertListEqual(self.__get_job_names(state.passed), self.__get_job_names(state2.passed))
        self.assertListEqual(self.__get_job_names(state.running), self.__get_job_names(state2.running))
    
    def test_state_change(self):
        
        sleeper = Sleeper(status_folder='tests/test_results/tasks')
        state = SchedulerState(waiting=[sleeper], status_folder='tests/test_results')

        self.assertListEqual(state.waiting, [sleeper])

        state.start_task(sleeper)
        self.assertListEqual(state.running, [sleeper])

        state.pass_task(sleeper)
        self.assertListEqual(state.passed, [sleeper])
