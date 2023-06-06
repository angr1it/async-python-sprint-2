from ..job import Job


class Sleeper(Job):
    def __init__(self, start_at="", max_working_time=-1, tries=0, dependencies=[]):
        pass

    def run(self):
        print('Sleeper sleeps..')
        return super().run()
