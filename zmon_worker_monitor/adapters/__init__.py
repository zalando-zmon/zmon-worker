#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
We provide a plugin system to load external functionality into Zmon.
This is needed as a way to decouple sensitive details -possibly proprietary- from the Zmon core.

The plugin system is separated in *adapters* and *implementations*.

Adapters are base classes that specify the behaviour of plugin types, they live in the subpackage
``zmon_worker.adapters``. All apaters inherit from ``zmon_worker.adapters.IBasePlugin``.
One adapter may have many implementations,

A plugin implementation needs 2 files:
1. a python source file containing a class that extends an adaptor.
2. a plugin info file. Text file with metadata to uniquely identify the plugin.


To see how this works let's look at the *resolution of the entities* to monitor:

What Zmon scheduler does is basically to periodically run some checks against some entities. An entity may be a host,
a database, a network port, or really anything you may want to run a check against. This means that the definition of
**entity** must be kept open and flexible, that's why the user must implement an entity plugin to define its entities.

How do you write an entity plugin to load your entities (hosts, databases, network ports, etc.)?

1. Write a python file containing a class that extends ``zmon_worker.adapters.ientity_plugin.IEntityPlugin``,
this class should contain your own logic for resolving your entities.

2. Write a text file with the same name of your python source but extension ``.scheduler_plugin``.

3. Place both files in a *plugin folder* of your choice and add that folder's absolute path to the
environment variable ``ZMON_PLUGINS``.


This description is quite dry, but we provide an example that will make things clearer:

The scheduler comes with one functional plugin, called ``entity_city``, this plugin provides *city entities*, these
are used for internal Zmon checks. This plugin is a simple template to start external plugin implementation.
The 2 files that implement the city plugin are ``zmon_scheduler.builtins.plugins.city.scheduler_plugin`` and
``zmon_scheduler.builtins.plugins.city.py``.

"""

from zmon_worker_monitor.adapters.ibase_plugin import IBasePlugin


__all__ = [
    'IBasePlugin',
]
