from ..job import Job

from ..exceptions import ExitLoopTaskCommand

class Breaker(Job):

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def as_dict(self):
        data = {
            'start_at': self.start_at,
            'max_working_time': self.max_working_time,
            'tries': self.tries,
            'dependencies': self.dependencies,
            'status_folder': self.status_folder
        }

        return data
    
    def _run(self, start_with = 0):
        self.logger.debug('Breaker: starts.')

        raise ExitLoopTaskCommand()
    
        yield 'none', None

        self.logger.debug('Breaker: leaves.')
