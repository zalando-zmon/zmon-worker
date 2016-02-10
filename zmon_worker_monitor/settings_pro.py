# -*- coding: utf-8 -*-
"""
Project settings for development:
 To customize the settings for a local environment please create another module called settings_local.py and change
 there the values you want, they will override the ones in this file
"""

LOGGING = {
    'version': 1,
    'disable_existing_loggers': True,
    'formatters': {
        'custom': {
            'format': '%(levelname)s [worker-%(process)d] %(name)s/%(funcName)s: %(message)s'
        },
    },

    'handlers': {
        'console': {
            'level': 'DEBUG',
            'class': 'logging.StreamHandler',
            'formatter': 'custom'
        },
    },

    'loggers': {
        '': {
            'handlers': ['console'],
            'propagate': True,
            'level': 'INFO',
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
            'custom': {
                'format': '%(levelname)s [server-%(process)d] %(name)s/%(funcName)s: %(message)s'
            },
        },
        'handlers': {
            'console': {
                'level': 'DEBUG',
                'class': 'logging.StreamHandler',
                'formatter': 'custom'
            },
        },
        'loggers': {
            '': {
                'handlers': ['console'],
                'propagate': True,
                'level': 'INFO',
            },
        }
    }
)
