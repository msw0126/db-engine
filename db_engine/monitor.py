from apscheduler.schedulers.blocking import BlockingScheduler
import logging
import sys, os, django
sys.path.append(os.path.dirname(os.path.realpath(sys.argv[0])))
os.environ['DJANGO_SETTINGS_MODULE'] = 'db_engine.settings'
django.setup()
from executor.TaskDetect import task_detect

logger = logging.getLogger("monitor")

logger.info('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))
scheduler = BlockingScheduler(daemonic=True)
scheduler.add_job(task_detect, 'interval', seconds=5)
try:
    scheduler.start()
except (KeyboardInterrupt, SystemExit):
    # Not strictly necessary if daemonic mode is enabled but should be done if possible
    scheduler.shutdown()
    logger.info('Exit The Job!')
