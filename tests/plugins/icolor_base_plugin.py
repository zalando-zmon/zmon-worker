#!/usr/bin/python
# -*- coding: utf-8 -*-


from zmon_worker_monitor.adapters.ibase_plugin import IBasePlugin

from abc import ABCMeta, abstractmethod
import colorsys


class IColorPlugin(IBasePlugin):

    """
    Example Base Plugin Interface (Adapter)
    Extend it to create a plugin to deal with trendy fashion colors in a country. :)
    """

    __metaclass__ = ABCMeta

    def __init__(self):
        super(IColorPlugin, self).__init__()
        self.country = None
        self.color_rgb = {
            'black': (0, 0, 0),
            'red': (255, 0, 0),
            'green': (0, 255, 0),
            'blue': (0, 0, 255),
            'violet': (238, 130, 238),
            'grey': (190, 190, 190),
            'yellow': (255, 255, 0),
            'brown': (165, 42, 42),
            'orange': (255, 127, 0),
            'white': (255, 255, 255),
        }

    @abstractmethod
    def get_season_colors(self):
        """
        Override to provide country specific code that return list of colors (r,g,b) that are popular this season

        :return: list(tuple(int(r),int(g),int(b)))
        """
        raise NotImplementedError

    def get_season_color_names(self):
        return [self.get_approx_name_of_color(r, g, b) for (r, g, b) in self.get_season_colors()]

    @classmethod
    def convert_rgb_to_hsv(cls, r, g, b):
        # just to put some logic inside the base class, lets convert from rgb to hsv
        norm = 255.0
        return colorsys.rgb_to_hsv(r/norm, g/norm, b/norm)

    def get_approx_name_of_color(self, r, g, b):
        """
        Give known color name that is closest to the rgb given
        """
        best_color = None
        min_dev = 3.0 * 256*256

        for name, (cr, cg, cb) in self.color_rgb.iteritems():
            dev = (cr-r)**2 + (cg-g)**2 + (cb-b)**2
            if dev < min_dev:
                best_color = name
                min_dev = dev

        return best_color


