class TimeOutGenerator(Exception):
    pass

class WaitForStart(Exception):
    pass

class TaskCompleted(Exception):
    pass

class NoJobsLeftWaiting(Exception):
    pass

class ContinueLoop(Exception):
    pass

class ExitLoopWithWaitingJobsLeft(Exception):
    pass

class SleeperFailed(Exception):
    pass

class ExitLoopTaskCommand(Exception):
    """Used specifically in order to break event loop from running task
    """
    pass