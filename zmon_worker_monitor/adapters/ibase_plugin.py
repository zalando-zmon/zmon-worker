#!/usr/bin/env python
# -*- coding: utf-8 -*-

from abc import ABCMeta, abstractmethod


class IBasePlugin(object):
    """
    Base class for all adapters (plugin types). Users should not extend this class directly.
    """

    __metaclass__ = ABCMeta

    def __init__(self):
        """
        Set the basic variables.
        """
        self.is_activated = False

    def activate(self):
        """
        Called at plugin activation.
        """
        self.is_activated = True

    def deactivate(self):
        """
        Called when the plugin is disabled.
        """
        self.is_activated = False

    @abstractmethod
    def configure(self, conf):
        """
        Called after plugin is loaded to pass the [configuration] section in their plugin info file
        :param conf: configuration dictionary
        """
        raise NotImplementedError

