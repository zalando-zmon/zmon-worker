# -*- coding: utf-8 -*-
import Queue

from mock import MagicMock

from zmon_worker_monitor.zmon_worker.common.utils import flatten, PeriodicBufferedAction


def test_periodic_buffered_action(monkeypatch):
    thread = MagicMock()
    sleep = MagicMock()

    monkeypatch.setattr('threading.Thread', thread)
    monkeypatch.setattr('time.sleep', sleep)

    data = {'foo': 'bar'}

    handle = {}

    def action(elems):
        assert [data] == elems
        if not handle.get('counter'):
            handle['counter'] = 1
            raise Exception('Fail the first time')
        handle['pba'].stop()

    pba = PeriodicBufferedAction(action=action, t_wait=0)
    handle['pba'] = pba
    pba.enqueue(data)
    pba.start()
    assert pba.is_active()
    pba._loop()


def test_periodic_buffered_action_retries_exceeded(monkeypatch):
    thread = MagicMock()
    sleep = MagicMock()

    monkeypatch.setattr('threading.Thread', thread)
    monkeypatch.setattr('time.sleep', sleep)

    data = 'FAIL-AND-DROP-ME'

    handle = {}

    def action(elems):
        assert [data] == elems
        if not handle.get('counter'):
            handle['counter'] = 1
        else:
            handle['pba'].stop()
        raise Exception('Fail always')

    pba = PeriodicBufferedAction(action=action, retries=1, t_wait=0)
    handle['pba'] = pba
    pba.enqueue(data)
    pba.start()
    pba._loop()


def test_periodic_buffered_action_queue_full():
    queue = MagicMock()
    queue.put_nowait.side_effect = Queue.Full()
    pba = PeriodicBufferedAction(action=None)
    pba._queue = queue
    pba.log = MagicMock()
    pba.enqueue('stuff')
    pba.log.exception.assert_called_with('Fatal Error: is worker out of memory? Details: ')


def test_periodic_buffered_action_loop_sleep(monkeypatch):
    handle = {}

    def sleep(s):
        assert 0.2 == s
        handle['slept'] = True
        handle['pba'].stop()

    monkeypatch.setattr('time.sleep', sleep)
    pba = PeriodicBufferedAction(action=None, t_random_fraction=0)
    handle['pba'] = pba
    pba.start()
    pba._loop()
    assert handle['slept']


def test_flatten_unicode():
    assert flatten({'a': {'b': 'c'}, 'd': 'e'}) == {'d': 'e', 'a.b': 'c'}
    assert flatten({'a': {'ü': 'c'}, 'd': 'e'}) == {'d': 'e', 'a.ü': 'c'}
    assert flatten({'a': {'ü'.decode("utf-8"): 'c'}, 'd': 'e'}) == {'d': 'e', 'a.ü': 'c'}
