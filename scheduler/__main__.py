from scheduler import Scheduler
from scheduler.tasks.sleeper import Sleeper

if __name__ == "__main__":
    job = Sleeper()
    sc = Scheduler()

    job.run()