#!/usr/bin/python
# -*- coding: utf-8 -*-


from tests.plugins.icolor_base_plugin import IColorPlugin
import logging


logger = logging.getLogger(__name__)


class ColorGermanyPlugin(IColorPlugin):

    """
    Example of a ColorPlugin for Germany.
    As all IColorPlugin it should provide a country unique way to get the trendy colors of the season.
    """

    def __init__(self):
        super(ColorGermanyPlugin, self).__init__()
        self.country = 'germany'
        self.main_fashion_sites = None

    def configure(self, conf):
        """
        This method is invoked automatically when the plugin is loaded, to inject external configuration.

        :param conf: (dict) Configuration parameters provided either in the [Configuration] section of the
                     plugin info file, or passed in the global_config argument to plugin_manager.collect_plugins()
        """
        extra_colors = conf.get('extra_colors', {})
        for name, (r, g, b) in extra_colors.iteritems():
            self.color_rgb[name] = (r, g, b)

        logger.debug('cool multi-line configuration value: fashion_sites: "%s"', conf.get('fashion_sites', ''))

        self.main_fashion_sites = conf.get('fashion_sites', '').split()

    def get_season_colors(self):
        """
        Example implementation: Lets pretend Tyrolean shorts are hot this season :)
        If this were real maybe get colors by scanning images posted in German fashion sites.
        """
        return [(169, 169, 169), (255, 250, 240), (5, 5, 5)]  # darkgray, floralwhite, grey2 (almost black)
