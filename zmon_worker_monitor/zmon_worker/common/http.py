from zmon_worker_monitor import __version__


def get_user_agent():
    return 'zmon-worker/{}'.format(__version__)
