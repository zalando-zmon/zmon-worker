#!/usr/bin/env python
# -*- coding: utf-8 -*-

from ibase_plugin import IBasePlugin
from abc import ABCMeta, abstractmethod


class IFunctionFactoryPlugin(IBasePlugin):
    """
    Base class for all plugins of type Function. We call it Function adapter.
    """

    __metaclass__ = ABCMeta

    def __init__(self):
        super(IFunctionFactoryPlugin, self).__init__()

    @abstractmethod
    def create(self, factory_ctx):
        """
        Automatically called to create the check function's object
        :param factory_ctx: creation context
        :return: an object that implements a check function
        """
        raise NotImplementedError


class ProtectedPartial(object):

    '''
    Provides functools.partial functionality with one additional feature: if keyword arguments contain '__protected'
    key with list of arguments as value, the appropriate values will not be overwritten when calling the partial. This
    way we can prevent user from overwriting internal zmon parameters in check command. The protected key uses double
    underscore to prevent overwriting it, we reject all commands containing double underscores.
    '''

    def __init__(self, func, *args, **kwargs):
        self.__func = func
        self.__partial_args = args
        self.__partial_kwargs = kwargs
        self.__protected = frozenset(kwargs.get('__protected', []))
        self.__partial_kwargs.pop('__protected', None)

    def __call__(self, *args, **kwargs):
        new_kwargs = self.__partial_kwargs.copy()
        new_kwargs.update((k, v) for (k, v) in kwargs.iteritems() if k not in self.__protected)
        return self.__func(*self.__partial_args + args, **new_kwargs)


def propartial(func, *args, **kwargs):
    '''
    >>> propartial(int, base=2)('100')
    4
    >>> propartial(int, base=2)('100', base=16)
    256
    >>> propartial(int, base=2, __protected=['base'])('100', base=16)
    4
    '''

    return ProtectedPartial(func, *args, **kwargs)