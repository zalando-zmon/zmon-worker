#!/usr/bin/python
# -*- coding: utf-8 -*-


from tests.plugins.icolor_base_plugin import IColorPlugin


class BadColorPlugin1(IColorPlugin):

    """
    Example of a ColorPlugin that will fail because configure() has not been implemented
    """

    def __init__(self):
        super(BadColorPlugin1, self).__init__()
        self.country = 'germany'
        self.main_fashion_sites = None


    def get_season_colors(self):
        """
        Example implementation of abstract method
        """
        return [(0, 0, 0)]

