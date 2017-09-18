# -*- coding: utf-8 -*-

import requests


def get_instance_identity_document():
    r = requests.get('http://169.254.169.254/latest/dynamic/instance-identity/document', timeout=3)
    return r.json()
