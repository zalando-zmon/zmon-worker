from zmon_worker_monitor import __version__


def get_user_agent():
    return 'zmon-worker/{}'.format(__version__)


def is_absolute_http_url(url):
    '''
    >>> is_absolute_http_url('')
    False

    >>> is_absolute_http_url('bla:8080/blub')
    False

    >>> is_absolute_http_url('https://www.zalando.de')
    True
    '''

    return url.startswith('http://') or url.startswith('https://')
