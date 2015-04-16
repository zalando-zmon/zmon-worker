
from zmon_worker_monitor.adapters.ibase_plugin import IBasePlugin

from abc import ABCMeta, abstractmethod
from collections import OrderedDict
from time import time, sleep
from threading import Thread

class ITemperaturePlugin(IBasePlugin):

    """
    Example Base Plugin Interface (Adapter)
    Extend it to create a plugin that connects to a device to periodically read its temperature.
    Who doesn't have a fully network connected kitchen these days :)
    """

    __metaclass__ = ABCMeta

    UNIT_CELSIUS = '_UNIT_CELSIUS_'
    UNIT_FAHRENHEIT = '_UNIT_FAHRENHEIT_'

    def __init__(self):
        super(ITemperaturePlugin, self).__init__()
        self.device = None
        self.unit = self.UNIT_CELSIUS
        self.readings = OrderedDict()
        self.stop = False
        self.interval = 0.01

    def start_update(self):
        self._th = Thread(target=self._update_loop)
        self._th.daemon = True
        self._th.start()

    @abstractmethod
    def read_temperature(self):
        """
        Called every interval to connect to device and get a new temperature reading
        """
        raise NotImplementedError

    @classmethod
    def convert_fahrenheit_to_celsius(cls, tf):
        # just to put some logic inside the base class
        return (tf - 32.0) * 5.0/9

    @classmethod
    def convert_celsius_to_fahrenheit(cls, tc):
        # just to put some logic inside the base class
        return tc * 9.0/5 + 32.0

    def get_temperature_average(self):
        return sum(self.readings.values()) / len(self.readings)

    def _update_loop(self):
        while not self.stop:
            self.readings[time()] = self.read_temperature()
            sleep(self.interval)
