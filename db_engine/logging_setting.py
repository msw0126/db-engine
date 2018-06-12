from datetime import datetime
from F_SETTING import LOG_DIR


LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'handlers': {
        'file_django': {
            'level': 'DEBUG',
            'class': 'logging.FileHandler',
            'filename': LOG_DIR+'django-'+datetime.now().strftime("%Y-%m-%d")+'.log',
            'formatter': 'verbose'
        },
        'file_monitor': {
            'level': 'DEBUG',
            'class': 'logging.FileHandler',
            'filename': LOG_DIR+'monitor-'+datetime.now().strftime("%Y-%m-%d")+'.log',
            'formatter': 'verbose'
        },
        'console': {
            'level': 'INFO',
            'class': 'logging.StreamHandler',
            'formatter': 'verbose'
        }
    },
    'loggers': {
        'django': {
            'handlers': ['file_django','console'],
            'level': 'DEBUG',
            'propagate': True,
        },
        'monitor' : {
            'handlers': ['file_monitor','console'],
            'level' : 'DEBUG',
            'propagate': True,
        }
    },
    'formatters': {
        'verbose': {
            'format': '%(levelname)s %(asctime)s %(module)s %(process)d %(thread)d %(message)s'
        },
        'simple': {
            'format': '%(levelname)s %(message)s'
        },
    },
}
