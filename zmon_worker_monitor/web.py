#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import cherrypy

import argparse
import settings
import logging

if __name__ == '__main__':
    import logging.config
    logging.config.dictConfig(settings.RPC_SERVER_CONF['LOGGING'])

logger = logging.getLogger(__name__)

# env vars get droped via zompy startup
os.environ["ORACLE_HOME"] = "/opt/oracle/instantclient_12_1/"
os.environ["LD_LIBRARY_PATH"] = os.environ.get("LD_LIBRARY_PATH", '') + ":/opt/oracle/instantclient_12_1/"

import rpc_server

DEFAULT_NUM_PROC = 16


def parse_args(args):
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config-file", help="path to config file")
    parser.add_argument('--no-rpc', action='store_true', help='Do not start XML-RPC server')
    return parser.parse_args(args)


def main(args=None):
    # add src dir to sys.path
    # src_dir = os.path.abspath(os.path.dirname(__file__))
    # if src_dir not in sys.path:
    #     sys.path.append(src_dir)

    args = parse_args(args)

    main_proc = rpc_server.MainProcess()

    # load cherrypy configuration
    if args.config_file and os.path.exists(args.config_file):
        cherrypy.config.update(args.config_file)
    elif os.path.exists('/app/web.conf'):
        cherrypy.config.update('/app/web.conf')
    else:
        cherrypy.config.update('web.conf')

    for key in cherrypy.config.keys():
        env_key = key.upper().replace('.', '_')
        if env_key in os.environ:
            cherrypy.config[key] = os.environ[env_key]

    # save cherrypy config in owr settings module
    settings.set_workers_log_level(cherrypy.config.get('loglevel', 'INFO'))
    settings.set_external_config(cherrypy.config)
    settings.set_rpc_server_port('2{}'.format('3500'))

    # start the process controller
    main_proc.start_proc_control()

    # start some processes per queue according to the config
    queues = cherrypy.config['zmon.queues']['local']
    for qn in queues.split(','):
        queue, N = (qn.rsplit('/', 1) + [DEFAULT_NUM_PROC])[:2]
        main_proc.proc_control.spawn_many(int(N), kwargs={"queue": queue, "flow": "simple_queue_processor"})

    if not args.no_rpc:
        main_proc.start_rpc_server()

    return main_proc


if __name__ == '__main__':
    main()
