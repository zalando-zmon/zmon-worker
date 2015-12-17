#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Our thin layer on top of yapsy_plugin to load external code
Folders to be explored are taken from the environment variable ZMON_PLUGINS

TODO: Add examples and point to the tests
"""

import logging
import os
import sys

from yapsy.PluginManager import PluginManagerSingleton
import pkg_resources

from zmon_worker_monitor.adapters.ifunctionfactory_plugin import IFunctionFactoryPlugin

logger = logging.getLogger(__name__)


# Some constants to define global behaviour

PLUGIN_INFO_EXT = 'worker_plugin'

PLUGIN_BUILTINS = ('zmon_worker_monitor.builtins.plugins',)

PLUGIN_ENV_VAR = 'ZMON_PLUGINS'

PLUGIN_CATEGORIES_FILTER = {
    'Function': IFunctionFactoryPlugin,
}

GLOBAL_CONFIG_PREFIX = 'plugin.{plugin_name}.'


class PluginError(Exception):
    pass


class PluginRecoverableError(PluginError):
    pass


class PluginFatalError(PluginError):
    pass


def _builtins_paths(subpackages, raise_errors=True):
    if not subpackages:
        return []
    folders = []

    for subpkg in (subpackages if isinstance(subpackages, (tuple, list)) else [subpackages]):
        try:
            parts = subpkg.split('.')
            path = pkg_resources.resource_filename('.'.join(parts[:-1]), parts[-1])
            if not os.path.isdir(path):
                raise Exception('path is not a directory: {}'.format(path))
        except Exception:
            logger.exception('erroneous plugins package: %s. Exception: ', subpkg)
            if raise_errors:
                _, ev, tb = sys.exc_info()
                raise PluginFatalError('Builtins plugins error in {}. Reason: {}'.format(subpkg, ev)), None, tb
        else:
            folders.append(path)

    return folders


def _env_dirs(env_var, raise_errors=True):
    env_value = os.environ.get(env_var)
    if not env_value:
        return []

    folders = []
    for d in env_value.split(os.pathsep):
        if not os.path.isdir(d):
            logger.warn('Wrong path %s in env variable %s', d, env_var)
            if raise_errors:
                raise PluginFatalError('Env plugins error in path: {}, from env_var: {}'.format(d, env_var))
            continue
        folders.append(d)
    return folders


def _filter_additional_dirs(path_list, raise_errors=True):
    if not path_list:
        return []
    folders = []
    for path in path_list:
        if os.path.isdir(path):
            folders.append(path)
        elif raise_errors:
            raise PluginFatalError('Additional dirs contains erroneous path: {}'.format(path))
    return folders


_initialized = {}


def init_plugin_manager(category_filter=None, info_ext=PLUGIN_INFO_EXT, builtins_pkg=PLUGIN_BUILTINS,
                        env_var=PLUGIN_ENV_VAR):
    """
    Initialize the plugin manager and set some behaviour options
    :param category_filter:
    :param info_ext:
    :param builtins_pkg:
    :param env_var:
    :return:
    """
    global _initialized

    # default category_filter is PLUGIN_CATEGORIES_FILTER (dict)
    category_filter = PLUGIN_CATEGORIES_FILTER if category_filter is None else category_filter

    logger.info('init plugin manager')

    manager = PluginManagerSingleton.get()

    manager.setCategoriesFilter(category_filter)
    manager.setPluginInfoExtension(info_ext)

    # save parameters used to initialize the module
    _initialized = dict(category_filter=category_filter, info_ext=info_ext, builtins_pkg=builtins_pkg, env_var=env_var)


def get_plugin_manager():
    """
    Get the plugin manager object (singleton)
    """
    return PluginManagerSingleton.get()


_collected = False


def collect_plugins(load_builtins=True, load_env=True, additional_dirs=None, global_config=None, raise_errors=True):
    """
    Collect plugins from folders in environment var and additional_dir param.

    :param plugin_env_var: environment variable containing a list of paths (shell $PATH style)
    :param additional_dirs: additional locations to search plugins in
    :return:
    """
    global _collected

    if not _initialized:
        raise PluginFatalError('You must invoke init_plugin_manager() before collect_plugins()!')
    if _collected:
        raise PluginFatalError('Plugins should be collected only once!')

    try:
        # load the plugins

        global_config = {} if global_config is None else global_config

        builtins = _builtins_paths(_initialized.get('builtins_pkg')) if load_builtins else []

        paths_env = _env_dirs(_initialized.get('env_var')) if load_env else []

        path_list = paths_env + _filter_additional_dirs(additional_dirs)

        # not necessary and may cause module name clashes... remove?
        # for entry in path_list:
        #    if entry not in sys.path:
        #        sys.path.append(entry)  # so the plugins can relatively import their submodules

        # check plugin dependencies declared in {plugin_dir}/requirements.txt are installed
        for path in path_list:
            miss_deps = _check_dependencies(path)
            if miss_deps:
                logger.error('Dependencies missing for plugin %s: %s', path, ','.join(miss_deps))
                if raise_errors:
                    raise PluginFatalError('Dependencies missing for plugin {}: {}'.format(path, ','.join(miss_deps)))

        manager = get_plugin_manager()
        manager.setPluginPlaces(builtins + path_list)

        # explore the provided locations and identify plugin candidates
        manager.locatePlugins()

        # save list of all plugin candidates: [(info file path, python file path, plugin info instance), ...]
        candidates = manager.getPluginCandidates()

        logger.debug('Recognized plugin candidates: %s', candidates)

        # trigger the loading of all plugin python modules
        manager.loadPlugins()

        all_plugins = manager.getAllPlugins()

        if len(all_plugins) != len(candidates):
            plugin_paths = map(_path_source_to_plugin, [p.path for p in all_plugins])

            dropped = [c for c in candidates if c[0] not in plugin_paths]
            logger.error('These plugin candidates have errors: %s', dropped)
            if raise_errors:
                raise PluginFatalError('Plugin candidates have errors: {}'.format(dropped))

        # configure and activate plugins

        for plugin in all_plugins:

            config_prefix = GLOBAL_CONFIG_PREFIX.format(plugin_name=plugin.name)

            conf_global = {}
            try:
                conf_global = {str(c)[len(config_prefix):]: v for c, v in global_config.iteritems() if str(c).startswith(config_prefix)}
                logger.debug('Plugin %s received global conf keys: %s', plugin.name, conf_global.keys())
            except Exception:
                logger.exception('Failed to parse global configuration. Reason: ')
                if raise_errors:
                    raise

            conf = {}
            try:
                if plugin.details.has_section('Configuration'):
                    conf = {c: v for c, v in plugin.details.items('Configuration')}  # plugin.plugin_info.detail has the safeconfig object
                logger.debug('Plugin %s received local conf keys: %s', plugin.name, conf.keys())
            except Exception:
                logger.exception('Failed to load local configuration from plugin: %s. Reason: ', plugin.name)
                if raise_errors:
                    raise

            # for security reasons our global config take precedence over the local config
            conf.update(conf_global)

            try:
                plugin.plugin_object.configure(conf)
            except Exception:
                logger.exception('Failed configuration of plugin: %s. Reason: ', plugin.name)
                if raise_errors:
                    raise
                plugin.plugin_object.deactivate()
                continue

            plugin.plugin_object.activate()

        _collected = True

    except PluginFatalError:
        raise

    except Exception:
        logger.exception('Unexpected error during plugin collection: ')
        if raise_errors:
            _, ev, tb = sys.exc_info()
            raise PluginFatalError("Error while loading plugins. Reason: {}".format(ev)), None, tb


def get_plugins_of_category(category, active=True, raise_errors=True):
    """
    Get plugins (plugin_info) of a given category
    """
    try:
        plugins = get_plugin_manager().getPluginsOfCategory(category)
    except KeyError:
        if raise_errors:
            raise PluginRecoverableError('Category {} not known to the plugin system'.format(category))
        return []
    if plugins and isinstance(active, bool):
        plugins = [p for p in plugins if p.is_activated == active]
    return plugins


def get_plugin_objs_of_category(category, active=True, raise_errors=True):
    """
    Get plugin objects of a given category
    """
    return [p.plugin_object for p in get_plugins_of_category(category, active, raise_errors)]


def get_plugin_by_name(name, category, not_found_is_error=True):
    """
    Get a plugin by name and category
    """
    plugin = get_plugin_manager().getPluginByName(name, category)

    if not plugin and not_found_is_error:
        raise PluginRecoverableError('Plugin by name {} not found under category {}'.format(name, category))

    return plugin


def get_plugin_obj_by_name(name, category, not_found_is_error=True):
    """
    Get a plugin object by name and category
    """
    plugin = get_plugin_by_name(name, category, not_found_is_error)
    return None if plugin is None else plugin.plugin_object


def get_all_plugins():
    """
    Get list of all loaded plugins
    """
    return get_plugin_manager().getAllPlugins()


def get_all_plugin_names():
    """
    Get list of names of all loaded plugins
    """
    plugins = get_all_plugins()
    if not plugins:
        return []
    return [p.name for p in plugins]


def get_all_categories():
    """
    Return list of categories provided in the category_filter
    """
    manager = get_plugin_manager()
    return manager.getCategories()


def get_loaded_plugins_categories():
    """
    Return list of categories that have one or more discovered plugins
    """
    return list(set([p.category for p in get_all_plugins()]))


def _check_dependencies(path):

    req_path = path + os.sep + 'requirements.txt'
    if not os.path.isfile(req_path):
        logger.debug('%s has no requirements.txt file' % path)
        return None

    missing_pkg = []
    with open(req_path) as f:
        for line in f:
            stripped = line.strip()
            if stripped and not stripped.startswith('#'):
                try:
                    pkg_resources.get_distribution(stripped)
                except Exception as _:
                    missing_pkg.append(stripped)
    return missing_pkg


def _path_source_to_plugin(source_path):
    source_no_py = source_path[:-3] if source_path.lower().endswith('.py') else source_path
    plugin_path = source_no_py + '.' + _initialized['info_ext']
    return plugin_path
