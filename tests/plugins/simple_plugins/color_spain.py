#!/usr/bin/python
# -*- coding: utf-8 -*-


from tests.plugins.icolor_base_plugin import IColorPlugin


class ColorSpainPlugin(IColorPlugin):

    """
    Example of a ColorPlugin for Germany.
    As all IColorPlugin it should provide a country unique way to get the trendy colors of the season.
    """

    def __init__(self):
        super(ColorSpainPlugin, self).__init__()
        self.country = 'spain'

    def configure(self, conf):
        """
        This method is invoked automatically when the plugin is loaded, to inject external configuration.

        :param conf: (dict) Configuration parameters provided either in the [Configuration] section of the
                     plugin info file, or passed in the global_config argument to plugin_manager.collect_plugins()
        :return:
        """
        extra_colors = conf.get('extra_colors', {})
        for name, (r, g, b) in extra_colors.iteritems():
            self.color_rgb[name] = (r, g, b)

    def get_season_colors(self):
        """
        Example implementation: Lets pretend bullfighter fashion is making a come back this season :)
        If this were real maybe get colors by scanning images posted in Spanish fashion sites.
        :return:
        """
        return [(176, 23, 31), (238, 238, 0), (28, 134, 238)]  # indian red, yellow 2, dodgerblue 2

