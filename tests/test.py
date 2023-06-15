import unittest

from schedule.job import Job
from schedule.scheduler import Scheduler   

class TestScheduler(unittest.TestCase):
    @unittest.expectedFailure
    def test_Scheduler_is_Job(self):
        scheduler = Scheduler()
        self.assertIsInstance(scheduler, Job)


if __name__ == '__main__':
    unittest.main()