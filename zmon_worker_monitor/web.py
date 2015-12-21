#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os

import argparse
import settings
import yaml
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


def read_config(path):
    with open(path) as fd:
        config = yaml.safe_load(fd)
    return config


def process_config(config):
    # If running on AWS, fetch the account number
    # TODO: fix this shell code and move it somewhere sane
    try:
        worker_account = subprocess.check_output('curl --connect-timeout 5 --silent http://169.254.169.254/latest/meta-data/iam/info/ | grep "ProfileArn" | grep -E -o "iam::([0-9]+)" | grep -E -o "[0-9]+"', shell=True)[:-1]
        config['account'] = 'aws:' + worker_account
    except:
        config['account'] = 'aws:error-during-startup'


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

    # save config in our settings module
    settings.set_workers_log_level(config.get('loglevel', 'INFO'))
    settings.set_external_config(config)
    settings.set_rpc_server_port('2{}'.format('3500'))

    # start the process controller
    main_proc.start_proc_control()

    # start some processes per queue according to the config
    queues = config['zmon.queues']['local']
    for qn in queues.split(','):
        queue, N = (qn.rsplit('/', 1) + [DEFAULT_NUM_PROC])[:2]
        main_proc.proc_control.spawn_many(int(N), kwargs={"queue": queue, "flow": "simple_queue_processor"})

    if not args.no_rpc:
        main_proc.start_rpc_server()

    return main_proc


if __name__ == '__main__':
    main()
