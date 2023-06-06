import unittest

from scheduler.job import Job
from scheduler.tasks.sleeper import Sleeper
from scheduler import Scheduler   

class TestSleeper(unittest.TestCase):
    
    def test_Sleeper_is_Job(self):
        job = Sleeper()
        self.assertIsInstance(job, Job)

if __name__ == '__main__':
    unittest.main()
