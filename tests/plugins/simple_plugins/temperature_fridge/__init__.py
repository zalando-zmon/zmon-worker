#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
This shows that your plugins can be python packages too
This is convenient if the logic have to be separated in several modules,
or to include extra data files together with the source
"""


from tests.plugins.itemperature_base_plugin import ITemperaturePlugin
from random import gauss
import logging


logger = logging.getLogger(__name__)


class TempFridgePlugin(ITemperaturePlugin):

    """
    Example of a TemperaturePlugin for a fridge.
    As an ITemperaturePlugin it should provide a way to read temperatures from a device.
    """

    def __init__(self):
        super(TempFridgePlugin, self).__init__()
        self.device = 'fridge'
        self.fridge_ip = None

    def configure(self, conf):
        """
        This method is invoked automatically when the plugin is loaded, to inject external configuration.

        :param conf: (dict) Configuration parameters provided either in the [Configuration] section of the
                     plugin info file, or passed in the global_config argument to plugin_manager.collect_plugins()
        """
        self.fridge_ip = conf.get('fridge_ip', '127.0.0.1')
        self.center = float(conf.get('center', '-100.0'))
        self.sigma = float(conf.get('sigma', '10'))

    def read_temperature(self):
        """
        Example implementation of the plugin abstract method.
        Returns random values that follow a normal distribution
        """
        return gauss(self.center, self.sigma)
