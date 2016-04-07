
import time
from mock import MagicMock

from zmon_worker_monitor.process_controller import register


def test_action_decorator(monkeypatch):
    """
    Test zmon_worker_monitor.process_controller.ProcAction
    Check1: methods are re-executed only after the waiting time specified
    Check2: different object instances call their respective bounded methods
    """

    # some values used in the checks
    t_wait_method_1 = 0.1
    t_wait_method_2 = 0.2
    some_flag=16

    class FibClass(object):
        """
        Object that produces an endless stream of possibly repeated numbers from the fibonacci series.
        Numbers increase only after wait_sec, as defined with the ProcAction decorator
        """

        def __init__(self):
            self.x0, self.x1 = -1, 1
            self.state = 0

        @register(wait_sec=t_wait_method_1, action_flag=some_flag)
        def next_fibo(self):
            self.x1, self.x0 = self.x1 + self.x0, self.x1
            # check the decorator makes action_flag available inside
            assert self.next_fibo.action_flag == some_flag
            return self.x1

        @register(wait_sec=t_wait_method_2)
        def message(self):
            return 'Slowly progressing fibonacci series x0=%d, x1=%d' % (self.x0, self.x1)

        @register(region='region1', wait_sec=t_wait_method_2)
        def state_str_with_side_effect1(self, a, b, c=None, d=None):
            self.state += 1
            return 'State1: %d. Args: %s, %s. Kwargs: c=%s, d=%s' % (self.state, a, b, c, d)

        @register('region2', wait_sec=t_wait_method_2)
        def state_str_with_side_effect2(self, a, b, c=None, d=None):
            self.state += 1
            return 'State2: %d. Args: %s, %s. Kwargs: c=%s, d=%s' % (self.state, a, b, c, d)

    #
    # Check that the first fibonacci nums are generated if enough time waited
    #

    # Instantiate 2 objects
    fib1 = FibClass()
    fib2 = FibClass()

    for n in (0, 1, 1, 2, 3, 5):
        first = fib1.next_fibo()
        second = fib1.next_fibo()
        assert first == second == fib1.next_fibo() == n  # but fast calls produce the same number
        if n < 5:
            time.sleep(t_wait_method_1)  # wait enough time for next_fibo to be updated in next call

    #
    # check ProcAction decorator correctly register the methods bound to each instance
    #

    methods1 = register.get_registered_by_obj(fib1)
    methods2 = register.get_registered_by_obj(fib2)

    for obj, methods in [(fib1, methods1), (fib2, methods2)]:
        print '====== methods of {} ======'.format(obj)
        for met in methods:
            print 'method: {}(), returned: {}'.format(met.__name__, met())

    # methods with same name are not equal because they are bounded to different instances
    assert not filter(None, [(met1 == met2 or met1() == met2()) for met1 in methods1 for met2 in methods2
                             if met1.__name__ == met2.__name__])

    #
    # check we can collect methods from different regions and they don't mix
    #

    methods1 = register.get_registered_by_obj(fib1)
    methods2 = register.get_registered_by_obj(fib2)
    assert set([m.__name__ for m in methods1]) == set([m.__name__ for m in methods2]) == {'next_fibo', 'message'}

    methods1 = register.get_registered_by_obj(fib1, region='region1')
    methods2 = register.get_registered_by_obj(fib2, region='region1')
    assert set([m.__name__ for m in methods1]) == set([m.__name__ for m in methods2]) == {'state_str_with_side_effect1'}

    methods1 = register.get_registered_by_obj(fib1, region='region2')
    methods2 = register.get_registered_by_obj(fib2, region='region2')
    assert set([m.__name__ for m in methods1]) == set([m.__name__ for m in methods2]) == {'state_str_with_side_effect2'}

    #
    # test that calls with different arguments create a new cache value
    #

    state = 0
    r1 = fib1.state_str_with_side_effect1(1, 2, c={3})  # First time call for this method on fib1
    r2 = fib1.state_str_with_side_effect1(1, 2, c={3})  # 2nd time should be cached
    state += 1
    assert r1 == r2 == 'State1: 1. Args: 1, 2. Kwargs: c=set([3]), d=None'

    for arg1 in [1, 2]:
        for d in [None, 'd', [11, 22, 33], {'a': 1, 'b': 2}, lambda x: x]:
            # even d=None will refresh cache, as decorator does not check method's default kwargs
            state += 1
            r3 = fib1.state_str_with_side_effect1(arg1, 2, c={3}, d=d)  # but no cache hit if we change the arguments
            assert r1 != r3 and r3 == 'State1: %s. Args: %s, 2. Kwargs: c=set([3]), d=%s' % (state, arg1, d)

    #
    # Check that we can invalidate cached regions, all methods from objects, and single methods
    #

    state = 0
    r1 = fib2.state_str_with_side_effect1(1, 2, c={69})  # First time call for this method on fib2
    r2 = fib2.state_str_with_side_effect1(1, 2, c={69})  # 2nd time should be cached
    state += 1
    assert r1 == r2 == 'State1: %s. Args: 1, 2. Kwargs: c=set([69]), d=None' % state

    # wait enough so cache must be refreshed
    time.sleep(t_wait_method_2)
    state += 1
    r3 = fib2.state_str_with_side_effect1(1, 2, c={69})  # this time cache should be refreshed
    assert r2 != r3 == 'State1: %s. Args: 1, 2. Kwargs: c=set([69]), d=None' % state

    # now lets invalidate the whole cache region and see that it is refreshed
    register.invalidate(region='region1')
    r4 = fib2.state_str_with_side_effect1(1, 2, c={69})  # this time cache should be refreshed
    r44 = fib2.state_str_with_side_effect1(1, 2, c={69})  # and here comes from cache again
    state += 1
    assert r3 != r4 == r44 == 'State1: %s. Args: 1, 2. Kwargs: c=set([69]), d=None' % state

    # now lets invalidate all methods from that object and see that it is refreshed
    register.invalidate(region='region1', obj=fib2)
    r5 = fib2.state_str_with_side_effect1(1, 2, c={69})  # this time cache should be refreshed
    r55 = fib2.state_str_with_side_effect1(1, 2, c={69})  # and here comes from cache again
    state += 1
    assert r4 != r5 == r55 == 'State1: %s. Args: 1, 2. Kwargs: c=set([69]), d=None' % state

    # now lets invalidate only this single method and see that it is refreshed
    register.invalidate(region='region1', obj=fib2, method=fib2.state_str_with_side_effect1)
    r6 = fib2.state_str_with_side_effect1(1, 2, c={69})  # this time cache should be refreshed
    r66 = fib2.state_str_with_side_effect1(1, 2, c={69})  # and here comes from cache again
    state += 1
    assert r5 != r6 == r66 == 'State1: %s. Args: 1, 2. Kwargs: c=set([69]), d=None' % state
