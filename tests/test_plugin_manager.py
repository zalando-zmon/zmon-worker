#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import traceback
import unittest
from mock import Mock, patch
import time

from zmon_worker_monitor.adapters.ifunctionfactory_plugin import IFunctionFactoryPlugin
from zmon_worker_monitor import plugin_manager
from plugins.icolor_base_plugin import IColorPlugin
from plugins.itemperature_base_plugin import ITemperaturePlugin


def simple_plugin_dir_abs_path(*suffixes):
    return os.path.abspath(os.path.join(os.path.dirname(__file__), 'plugins/simple_plugins', *suffixes))


def broken_plugin_dir_abs_path(*suffixes):
    return os.path.abspath(os.path.join(os.path.dirname(__file__), 'plugins/broken_plugins', *suffixes))


class TestPluginManager(unittest.TestCase):


    def test_load_plugins_twice(self):
        """
        Test that exception is raised if you collect plugins more than once
        """
        # reload the plugin
        reload(plugin_manager)

        plugin_manager.init_plugin_manager()  # init plugin manager

        plugin_manager.collect_plugins(load_builtins=True, load_env=False, additional_dirs=None)

        with self.assertRaises(plugin_manager.PluginFatalError):
            plugin_manager.collect_plugins(load_builtins=True, load_env=False, additional_dirs=None)


    def test_load_builtin_plugins(self):
        """
        Test that city plugin can be fully loaded
        """
        # reload the plugin
        reload(plugin_manager)

        plugin_manager.init_plugin_manager()  # init the plugin manager

        # collect only builtin plugins
        plugin_manager.collect_plugins(load_builtins=True, load_env=False, additional_dirs=None)

        # city plugin is a builtin and is in category entity
        plugin_name = 'http'
        plugin_category = 'Function'

        self.assertIn(plugin_name, plugin_manager.get_all_plugin_names(), 'http plugin name must be found')

        http_plugin = plugin_manager.get_plugin_by_name(plugin_name, plugin_category)

        self.assertIsNotNone(http_plugin, 'http plugin must be under category Function')
        self.assertEqual(http_plugin.name, plugin_name, 'check plugin name field')

        # check city plugin object
        self.assertTrue(hasattr(http_plugin, 'plugin_object'), 'http.plugin_object exists')
        self.assertIsNotNone(http_plugin.plugin_object, 'http.plugin_object is not None')
        self.assertTrue(isinstance(plugin_manager.get_plugin_obj_by_name(plugin_name, plugin_category), IFunctionFactoryPlugin),
                        'the entity plugin object is instance of IFunctionFactoryPlugin')

        # check that city plugin object is activated
        self.assertTrue(http_plugin.is_activated)
        self.assertEqual(http_plugin.plugin_object.is_activated, http_plugin.is_activated)

        # check that the city plugin object was configured with the path to a data file
        #self.assertIsNotNone(http_plugin.plugin_object.path, 'city was configured with the path to a data file')

        # check that the city plugin can load its entities data
        # has_tokyo = False
        # try:
        #     entities = http_plugin.plugin_object._get_entities()
        #     has_tokyo = bool([1 for cdata in entities if cdata['city'] == 'tokyo'])
        # except Exception:
        #     pass
        # self.assertTrue(has_tokyo, 'cities loaded and tokyo is among them')

    @patch.dict(os.environ, {'ZMON_PLUGINS': simple_plugin_dir_abs_path()})
    def test_load_plugins_several_categories(self):
        """
        Test is we can load and correctly locate plugins from several categories
        First it explores folders from ZMON_PLUGINS env_var, and then from additional_dirs
        """
        for test_load_from in ('env_var', 'additional_folders'):

            # reload the plugin
            reload(plugin_manager)

            # Lets create a category filter that includes our builtin plugin type and 2 types we defines for our tests
            category_filter = {
                'Function': IFunctionFactoryPlugin,
                'Color': IColorPlugin,
                'Temperature': ITemperaturePlugin,
            }

            if test_load_from == 'env_var':
                # init the plugin manager
                plugin_manager.init_plugin_manager(category_filter=category_filter)

                # collect plugins builtin and explore env_var: ZMON_PLUGINS="/.../tests/plugins/simple_plugins"
                plugin_manager.collect_plugins(load_builtins=True, load_env=True, additional_dirs=None)

            elif test_load_from == 'additional_folders':
                # init the plugin manager
                plugin_manager.init_plugin_manager(category_filter=category_filter)

                test_plugin_dir = simple_plugin_dir_abs_path()

                # collect plugins builtin and explore  additional_dirs: /.../tests/plugins/simple_plugins
                plugin_manager.collect_plugins(load_builtins=True, load_env=False, additional_dirs=[test_plugin_dir])

            # check categories

            all_categories = plugin_manager.get_all_categories()
            seen_categories = plugin_manager.get_loaded_plugins_categories()

            self.assertEqual(set(all_categories), set(category_filter.keys()), 'All defined categories are stored')
            self.assertTrue(len(seen_categories) >= 2 and set(seen_categories).issubset(set(all_categories)),
                            'found at least 2 categories and they all belong to all defined categories')

            # check known test plugins are loaded

            known_plugin_names = ['http', 'color_spain', 'color_germany', 'temperature_fridge']
            plugin_names = plugin_manager.get_all_plugin_names()

            # print 'known_plugin_names', known_plugin_names
            # print 'plugin_names', plugin_names


            self.assertTrue(set(known_plugin_names).issubset(plugin_names), 'All known test plugins are loaded')

            # test get_plugin_obj_by_name() and get_plugin_objs_of_category()

            color_ger = plugin_manager.get_plugin_by_name('color_germany', 'Color')
            color_ger_obj = plugin_manager.get_plugin_obj_by_name('color_germany', 'Color')
            self.assertEqual(id(color_ger.plugin_object), id(color_ger_obj), 'locate plugin object works')
            self.assertEqual(color_ger.plugin_object.country, 'germany', 'located object field values look good')
            all_color_objs = plugin_manager.get_plugin_objs_of_category('Color')
            self.assertEqual(id(color_ger_obj), id([obj for obj in all_color_objs if obj.country == 'germany'][0]),
                             'locate the plugin object in a convoluted way works too')

            # test that color_german plugin was configured with the main fashion sites

            conf_sites_germany = ['www.big_fashion_site.de', 'www.other_fashion_site.de']

            self.assertTrue(set(conf_sites_germany) == set(color_ger_obj.main_fashion_sites), 'object is configured')

            # test that plugin objects run its logic correctly

            color_obj_de = plugin_manager.get_plugin_obj_by_name('color_germany', 'Color')
            color_obj_es = plugin_manager.get_plugin_obj_by_name('color_spain', 'Color')

            simple_colors_de = ['grey', 'white', 'black']
            simple_colors_es = ['brown', 'yellow', 'blue']

            col_names_de = color_obj_de.get_season_color_names()
            col_names_es = color_obj_es.get_season_color_names()

            self.assertEqual(col_names_de, simple_colors_de)
            self.assertEqual(col_names_es, simple_colors_es)

            # Test also the logic of temperature plugin object, this simulates a bit more complex logic
            # Temp readings are simulated as a normal distribution centered at -5 and 0.2 sigma (values from config)
            # we spawn the thread that periodically do temp reading, we wait some intervals and then get the avg temp
            # Finally we check that T avg is -5 +- 10 sigmas (see local config)

            temp_fridge = plugin_manager.get_plugin_obj_by_name('temperature_fridge', 'Temperature')
            temp_fridge.start_update()
            time.sleep(temp_fridge.interval * 20)  # we wait for some temp collection to happen
            temp_fridge.stop = True
            tavg = temp_fridge.get_temperature_average()
            # This test is non-deterministic, but probability of failure is super small, so in practice it is ok
            self.assertTrue(abs(-5.0 - tavg) < 0.2 * 10, 'the avg temperature is close to -5')

            # test subpackage dependencies can be resolved
            self.assertEqual(temp_fridge.engine.power_unit, 'Watts')

    @patch.dict(os.environ, {'ZMON_PLUGINS': simple_plugin_dir_abs_path()})
    def test_global_config(self):
        """
        Test that the plugin can configure itself from the global config and that global config
        takes precedence over local config
        """
        # reload the plugin
        reload(plugin_manager)

        # Lets create a category filter that includes our builtin plugin type and 2 types we defines for our tests
        category_filter = {
            'Function': IFunctionFactoryPlugin,
            'Color': IColorPlugin,
            'Temperature': ITemperaturePlugin,
        }

        # init the plugin manager
        plugin_manager.init_plugin_manager(category_filter=category_filter)

        # inject as global conf to color_german plugin fashion sites different from the local conf
        global_conf = {
            'plugin.color_germany.configuration.fashion_sites': 'superfashion.de hypefashion.de',
            'plugin.other_plugin.configuration.otherkey': 'this will not be passed to color_germany.configure',
        }

        # collect plugins builtin and explore env_var: ZMON_PLUGINS="/.../tests/plugins/simple_plugins"
        plugin_manager.collect_plugins(load_builtins=True, load_env=True, additional_dirs=None,
                                       global_config=global_conf)

        # test that color_german plugin was configured according to the global conf

        global_conf_sites = ['superfashion.de', 'hypefashion.de']

        color_ger_obj = plugin_manager.get_plugin_obj_by_name('color_germany', 'Color')

        self.assertTrue(set(global_conf_sites) == set(color_ger_obj.main_fashion_sites), 'object is configured')

    @patch.dict(os.environ, {'ZMON_PLUGINS': simple_plugin_dir_abs_path()})
    def test_load_broken_plugins(self):
        """
        Test that we fail predictably on bad plugins and check that we propagate in the exception info to where
        the error is coming from, either in the exception message or in its traceback
        """

        for plugin_dir in 'bad_plugin1', 'bad_plugin2', 'bad_plugin3':

            plugin_abs_dir = broken_plugin_dir_abs_path(plugin_dir)

            # reload the plugin
            reload(plugin_manager)

            # Lets create a category filter that includes our builtin plugin type and 2 types we defines for our tests
            category_filter = {
                'Function': IFunctionFactoryPlugin,
                'Color': IColorPlugin,
                'Temperature': ITemperaturePlugin,
            }

            # init the plugin manager
            plugin_manager.init_plugin_manager(category_filter=category_filter)

            is_raised = False
            our_plugins_words = ['bad_color', 'badcolor', 'badplugin', 'bad_plugin']
            try:
                # collect plugins should fail with our custom fatal exception
                plugin_manager.collect_plugins(load_builtins=True, load_env=True, additional_dirs=[plugin_abs_dir])
            except plugin_manager.PluginError as e:
                is_raised = True
                exec_all_str = (str(e) + traceback.format_exc()).lower()
                self.assertTrue(any(s in exec_all_str for s in our_plugins_words),
                                'Exception info and/or traceback point you to the failing plugin')

            self.assertTrue(is_raised)

    @patch.dict(os.environ, {'ZMON_PLUGINS': simple_plugin_dir_abs_path()})
    def test_plugins_unsatisfied_requirements(self):
        """
        Test that we recognize missing dependencies in requirements.txt files in plugins dirs
        """

        plugin_abs_dir = broken_plugin_dir_abs_path('plugin_dir_with_requirements')

        # reload the plugin
        reload(plugin_manager)

        # Lets create a category filter that includes our builtin plugin type and 2 types we defines for our tests
        category_filter = {
            'Function': IFunctionFactoryPlugin,
            'Color': IColorPlugin,
            'Temperature': ITemperaturePlugin,
        }

        # init the plugin manager
        plugin_manager.init_plugin_manager(category_filter=category_filter)

        # test that we detect all missing dependencies in requirements.txt

        is_raised = False
        requirements = ('some_impossible_dependency', 'other_impossible_dependency', 'yet_another_dependency')
        try:
            # collect only builtin plugins should fail with our custom fatal exception
            plugin_manager.collect_plugins(load_builtins=True, load_env=True, additional_dirs=[plugin_abs_dir])

        except plugin_manager.PluginError as e:
            is_raised = True
            for miss_dep in requirements:
                self.assertTrue(miss_dep in str(e), 'Missing dependency in requirement file is discovered')

        self.assertTrue(is_raised)

if __name__ == '__main__':
    unittest.main()
