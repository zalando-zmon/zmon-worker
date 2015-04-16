#!/usr/bin/python
# -*- coding: utf-8 -*-


from tests.plugins.icolor_base_plugin import IColorPlugin


class BadColorPlugin3(IColorPlugin):

    """
    Example of a ColorPlugin that will fail because we generate an exception on initialization
    """

    def __init__(self):
        super(BadColorPlugin3, self).__init__()
        self.country = 'germany'
        self.main_fashion_sites = None
        # artificially produce a ZeroDivisionError on initialization
        fractions = [1.0/i for i in range(3, -1, -1)]

    def configure(self, conf):
        """
        This method is invoked automatically when the plugin is loaded, to inject external configuration.

        :param conf: (dict) Configuration parameters provided either in the [Configuration] section of the
                     plugin info file, or passed in the global_config argument to plugin_manager.collect_plugins()
        """
        self.main_fashion_sites = conf.get('fashion_sites', '').split()

    def get_season_colors(self):
        """
        Example implementation of abstract method
        """
        return [(0, 0, 0)]

