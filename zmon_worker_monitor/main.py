#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import argparse
import yaml
import logging
import logging.config
import warnings

import requests
from requests.packages.urllib3.exceptions import SubjectAltNameWarning

import settings
import plugin_manager
import rpc_server
from .flags import MONITOR_RESTART, MONITOR_KILL_REQ, MONITOR_PING
from .web_server.start import start_web


warnings.filterwarnings('ignore', category=SubjectAltNameWarning)


# env vars get droped via zompy startup
os.environ["ORACLE_HOME"] = "/opt/oracle/instantclient_12_1/"
os.environ["LD_LIBRARY_PATH"] = os.environ.get("LD_LIBRARY_PATH", '') + ":/opt/oracle/instantclient_12_1/"

DEFAULT_NUM_PROC = 16


def parse_args(args):
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config-file", help="path to config file")
    parser.add_argument('--no-rpc', action='store_true', help='Do not start XML-RPC server')
    return parser.parse_args(args)


def read_config(path):
    with open(path) as fd:
        config = yaml.safe_load(fd)
    return config


def process_config(config):
    # If running on AWS, fetch the account number
    try:
        iam_info = requests.get('http://169.254.169.254/latest/meta-data/iam/info/', timeout=3).json()
        account_id = iam_info['InstanceProfileArn'].split(':')[4]
        config['account'] = 'aws:' + account_id

        resp = requests.get('http://169.254.169.254/latest/meta-data/placement/availability-zone', timeout=3)
        config['region'] = resp.text[:-1]
    except Exception:
        logging.warning('Failed to retrieve AWS account info.')
        config['account'] = 'aws:error-during-startup'
        config['region'] = 'unknown'


def main(args=None):

    args = parse_args(args)

    main_proc = rpc_server.MainProcess()

    config = {}

    # load default configuration from file
    for path in (args.config_file, 'config.yaml'):
        if path and os.path.exists(path):
            config = read_config(path)
            break

    process_config(config)

    # allow overwritting any configuration setting via env vars
    for k, v in os.environ.items():
        if k.startswith('WORKER_'):
            config[k.replace("WORKER_", "").replace("_", ".").lower()] = v

    # make zmon worker compatible with old redis config vars
    if 'redis.host' in config:
        port = config.get('redis.port', 6379)
        config.update({"redis.servers": '{}:{}'.format(config["redis.host"], port)})

    # save config in our settings module
    settings.set_workers_log_level(config.get('loglevel', 'INFO'))
    settings.set_external_config(config)
    settings.set_rpc_server_port(config.get('server.port'))

    logging.config.dictConfig(settings.RPC_SERVER_CONF['LOGGING'])

    logger = logging.getLogger(__name__)

    # start the process controller
    main_proc.start_proc_control()

    # start web server process under supervision
    main_proc.proc_control.spawn_process(
        target=start_web,
        kwargs=dict(
            listen_on=config.get('webserver.listen_on', '0.0.0.0'),
            port=int(config.get('webserver.port', '8080')),
            log_conf=None,
            threaded=True,
            rpc_url='http://{host}:{port}{path}'.format(host='localhost', port=config.get('server.port'),
                                                        path=settings.RPC_SERVER_CONF['RPC_PATH']),
        ),
        flags=MONITOR_RESTART,  # web server will be restarted if dies
    )

    # init the plugin manager
    plugin_manager.init_plugin_manager()

    # load external plugins (should be run only once)
    plugin_manager.collect_plugins(global_config=config, load_builtins=True, load_env=True)

    # start worker processes per queue according to the config
    queues = config['zmon.queues']
    for qn in queues.split(','):
        queue, N = (qn.rsplit('/', 1) + [DEFAULT_NUM_PROC])[:2]
        main_proc.proc_control.spawn_many(
            int(N),
            kwargs={
                'queue': queue,
                'flow': 'simple_queue_processor',
                'tracer': config.get('opentracing.tracer'),
            },
            flags=MONITOR_RESTART | MONITOR_KILL_REQ | MONITOR_PING)

    if not args.no_rpc:
        try:
            main_proc.start_rpc_server()
        except (KeyboardInterrupt, SystemExit):
            logger.info('RPC server stopped. Exiting main')

    return main_proc


if __name__ == '__main__':
    main()
