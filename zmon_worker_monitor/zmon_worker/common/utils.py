import logging
import Queue
import random
import threading
import time

import psutil


def flatten(structure, key='', path='', flattened=None):
    '''
    >>> flatten({})
    {}
    >>> flatten({'a': {'b': {'c': ['d', 'e']}}})
    {'a.b.c': ['d', 'e']}
    >>> sorted(flatten({'a': {'b': 'c'}, 'd': 'e'}).items())
    [('a.b', 'c'), ('d', 'e')]
    '''
    path = path.encode("utf-8") if isinstance(path, unicode) else str(path)
    key = key.encode("utf-8") if isinstance(key, unicode) else str(key)

    if flattened is None:
        flattened = {}
    if not isinstance(structure, dict):
        flattened[((path + '.' if path else '')) + key] = structure
    else:
        for new_key, value in structure.items():
            flatten(value, new_key, '.'.join(filter(None, [path, key])), flattened)
    return flattened


class PeriodicBufferedAction(object):
    def __init__(self, action, action_name=None, retries=5, t_wait=10, t_random_fraction=0.5):
        self.log = logging.getLogger(__name__)
        self._stop = True
        self.action = action
        self.action_name = action_name if action_name else (action.func_name if hasattr(action, 'func_name') else
                                                            (action.__name__ if hasattr(action, '__name__') else None))
        self.retries = retries
        self.t_wait = t_wait
        self.t_rand_fraction = t_random_fraction

        self._queue = Queue.Queue()
        self._thread = threading.Thread(target=self._loop)
        self._thread.daemon = True

    def start(self):
        self._stop = False
        self._thread.start()

    def stop(self):
        self._stop = True

    def is_active(self):
        return not self._stop

    def get_time_randomized(self):
        return self.t_wait * (1 + random.uniform(-self.t_rand_fraction, self.t_rand_fraction))

    def enqueue(self, data, count=0):
        elem = {
            'data': data,
            'count': count,
            # 'time': time.time()
        }
        try:
            self._queue.put_nowait(elem)
        except Queue.Full:
            self.log.exception('Fatal Error: is worker out of memory? Details: ')

    def _collect_from_queue(self):
        elem_list = []
        empty = False

        while not empty and not self._stop:
            try:
                elem_list.append(self._queue.get_nowait())
            except Queue.Empty:
                empty = True
        return elem_list

    def _loop(self):
        t_last = time.time()
        t_wait_last = self.get_time_randomized()

        while not self._stop:
            if time.time() - t_last >= t_wait_last:
                elem_list = self._collect_from_queue()
                try:
                    if elem_list:
                        self.action([e['data'] for e in elem_list])
                except Exception as e:
                    self.log.error('Error executing action %s: %s', self.action_name, e)
                    for elem in elem_list:
                        if elem['count'] < self.retries:
                            self.enqueue(elem['data'], count=elem['count'] + 1)
                        else:
                            self.log.error('Error: Maximum retries reached for action %s. Dropping data: %s ',
                                           self.action_name, elem['data'])
                finally:
                    t_last = time.time()
                    t_wait_last = self.get_time_randomized()
            else:
                # so loop is responsive to stop commands
                time.sleep(0.2)


def get_process_cmdline(pid):
    try:
        # Some OSes report cmdline differently - join for 'zmon-worker check 999'...
        return ' '.join(filter(bool, psutil.Process(pid).cmdline()))
    except: # noqa
        return 'N/A'
