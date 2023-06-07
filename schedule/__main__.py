import logging

from .scheduler import Scheduler
from schedule.tasks.sleeper import Sleeper
from .worker import Worker
from multiprocessing import Process

logger = logging.getLogger()
logging.basicConfig(
    handlers=[logging.FileHandler(filename='app.log', mode='w')],
    level=logging.DEBUG,
    format='%(process)d: %(asctime)s: %(levelname)s - %(message)s'
)


if __name__ == "__main__":
    logger.debug('Main: starts.')
    scheduler = Scheduler()
    p1 = Worker(scheduler)
    p2 = Worker(scheduler)
    scheduler.schedule(Sleeper())
    scheduler.schedule(Sleeper(2))
    scheduler.schedule(Sleeper(3))
    scheduler.schedule(Sleeper(4))
    scheduler.schedule(Sleeper(5))
    scheduler.schedule(Sleeper(6))
    p1.start()
    p2.start()

    logger.debug('Main: waits for jsoin.')
    p1.join()
    p2.join()

    logger.debug('Main: exits.')