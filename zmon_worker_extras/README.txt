
# README


This folder contains extra content for for Zmon Worker. Each subdirectory for a different type of plugin.

These plugins are *not* part of zmon-worker deployable package.
If you want zmon-worker to discover them copy them and add their
location to the ZMON_PLUGINS environment variable
(we do this in the Dockerfile, so our docker image will discover this plugins).

Note that you must add each subfolder to ZMON_PLUGINS env variable,
separated by the path separator character (':' in unix)

Contains the following plugin types:

1. check_plugins/

    Zmon checks plugins (Category: Function, Type: IFunctionFactoryPlugin).
    They are not included in {zmon-package}/builtins/plugins/
    because they are not 100% supported, Unit Tested, or contain legacy code.

