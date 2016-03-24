#!/usr/bin/env python
# -*- coding: utf-8 -*-


"""
Define bitwise flags to signal activation of some features.
Flag constants most be powers of 2 and unique.
"""


#
# Begin FLAG constant declaration
#

# Process health monitored and restarted if it dies
MONITOR_RESTART = 1 << 0

# Process will periodically give us a ping with info of its state
MONITOR_PING = 1 << 1

# Process is allowed to request his own termination
MONITOR_KILL_REQ = 1 << 2

# Process should not be monitored
MONITOR_NONE = 1 << 3

#
# end FLAG declaration
#


__flag_dict = None


# helper functions to operate on the flags constants


def flag_dict():
    global __flag_dict
    if __flag_dict is None:
        __flag_dict = {f: v for f, v in vars().items() if f.isupper() and not (f.startswith('__') or callable(v)) and
                       __is_pow2(v)}
    return __flag_dict


def num2flags(number):
    return [v for v in flag_dict().values() if has_flag(number, v)]


def flags2num(flag_list):
    return reduce(lambda x, y: x | y, flag_list)


def has_flag(number, flag):
    return number & flag == flag


def __is_pow2(x):
    i = x if str(x).isdigit() else -1
    return True if i > 0 and i & (i-1) == 0 else False
