
import time
from mock import MagicMock

from zmon_worker_monitor.process_controller import ProcAction


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

        @ProcAction(wait_sec=t_wait_method_1, action_flag=some_flag)
        def next_fibo(self):
            self.x1, self.x0 = self.x1 + self.x0, self.x1
            # check the decorator makes action_flag available inside
            assert self.next_fibo.action_flag == some_flag
            return self.x1

        @ProcAction(wait_sec=t_wait_method_2)
        def message(self):
            return 'Slowly progressing fibonacci series x0=%d, x1=%d' % (self.x0, self.x1)

    # Instantiate 2 objects
    fib1 = FibClass()
    fib2 = FibClass()

    # Check that the first fibonacci nums are generated if enough time waited
    for n in (0, 1, 1, 2, 3, 5):
        first = fib1.next_fibo()
        second = fib1.next_fibo()
        assert first == second == fib1.next_fibo() == n  # but fast calls produce the same number

        if n < 5:
            time.sleep(t_wait_method_1)  # wait enough time for next_fibo to be updated in next call

    # check ProcAction decorator correctly register the methods bound to each instance
    methods1 = ProcAction.get_registered_by_obj(fib1)
    methods2 = ProcAction.get_registered_by_obj(fib2)

    for obj, methods in [(fib1, methods1), (fib2, methods2)]:
        print '====== methods of {} ======'.format(obj)
        for met in methods:
            print 'method: {}(), returned: {}'.format(met.__name__, met())

    # methods with same name are not equal because they are bounded to different instances
    assert not filter(None, [(met1 == met2 or met1() == met2()) for met1 in methods1 for met2 in methods2
                             if met1.__name__ == met2.__name__])





