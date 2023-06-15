

class TimeOutGenerator(Exception):
    pass

class WaitForStart(Exception):
    pass

class TaskCompleted(Exception):
    """Task already completed previously.
    """
    pass

class NoJobsLeftWaiting(Exception):
    pass

class ContinueLoop(Exception):
    pass

class ExitLoopWithWaitingJobsLeft(Exception):
    pass

class SleeperFailed(Exception):
    pass