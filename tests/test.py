import unittest

from schedule.job import Job
from schedule.tasks.sleeper import Sleeper
from schedule.scheduler import Scheduler   

class TestSleeper(unittest.TestCase):
    
    def test_Sleeper_is_Job(self):
        job = Sleeper()
        self.assertIsInstance(job, Job)

    @unittest.expectedFailure
    def test_Sleeper_is_Scheduler(self):
        job = Sleeper()
        self.assertIsInstance(job, Scheduler)

if __name__ == '__main__':
    unittest.main()