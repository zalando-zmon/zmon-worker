# -*- coding: utf-8 -*-
"""
Project settings for development:
 To customize the settings for a local environment please create another module called settings_local.py and change
 there the values you want, they will override the ones in this file
"""

import os

app_home = os.path.abspath(os.environ['APP_HOME'] if 'APP_HOME' in os.environ else './')

data_dir = os.path.abspath(os.path.join(app_home, 'zmon_worker_data'))

# application data folder needs to be created by the application itself
for d in (data_dir, ):
    if not os.path.isdir(d):
        os.mkdir(d)

LOGGING = {
    'version': 1,
    'disable_existing_loggers': True,
    'formatters': {
        'verbose': {
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(module)s - %(process)d - %(thread)d - %(message)s'
        },
        'custom': {
            'format': '%(asctime)s - %(levelname)s - %(name)s - %(funcName)s - %(message)s'
        },
    },

    'handlers': {

        'console': {
            'level': 'INFO',
            'class': 'logging.StreamHandler',
            'formatter': 'custom'
        },

    },

    'loggers': {
        '': {
            'handlers': ['console'],
            'propagate': True,
            'level': 'DEBUG',
        },
    }
}


RPC_SERVER_CONF = dict(

    HOST='localhost',

    PORT=8500,

    RPC_PATH='/zmon_rpc',

    LOGGING={
        'version': 1,
        'disable_existing_loggers': True,
        'formatters': {
            'verbose': {
                'format': '%(asctime)s - %(name)s - %(levelname)s - %(module)s - %(process)d - %(thread)d - %(message)s'
            },
            'custom': {
                'format': '%(asctime)s - %(levelname)s - %(name)s - %(funcName)s - %(message)s'
            },
        },
        'handlers': {

            'console': {
                'level': 'INFO',
                'class': 'logging.StreamHandler',
                'formatter': 'custom'
            },

        },
        'loggers': {
            '': {
                'handlers': ['console'],
                'propagate': True,
                'level': 'DEBUG',
            },
        }
    }
)
