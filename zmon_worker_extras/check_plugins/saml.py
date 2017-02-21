#!/usr/bin/env python
# -*- coding: utf-8 -*-

import codecs
import requests
import os

from bs4 import BeautifulSoup
from zmon_worker_monitor.zmon_worker.errors import CheckError
from zmon_worker_monitor.adapters.ifunctionfactory_plugin import IFunctionFactoryPlugin, propartial


class SAMLFactory(IFunctionFactoryPlugin):
    def __init__(self):
        super(SAMLFactory, self).__init__()

    def configure(self, conf):
        """
        Called after plugin is loaded to pass the [configuration] section in their plugin info file
        :param conf: configuration dictionary
        """
        self.saml_url = conf.get('url')
        self.username = conf.get('username')
        self.__password = conf.get('password')
        return

    def create(self, factory_ctx):
        """
        Automatically called to create the check function's object
        :param factory_ctx: (dict) names available for Function instantiation
        :return: an object that implements a check function
        """
        return propartial(SAMLWrapper, url=self.saml_url, user=self.username, password=self.__password)


class SAMLError(CheckError):
    def __init__(self, message):
        self.message = message
        super(SAMLError, self).__init__()

    def __str__(self):
        return 'SAML Error. Message: {}'.format(self.message)


class SAMLWrapper(object):
    def __init__(
            self,
            url,
            user=None,
            password=None,
            params=None,
            timeout=10,
            max_retries=0,
            verify=True,
            headers=None,
            ):
        self.url = url
        self.params = params or {}
        self.headers = headers or {}
        self.username = user
        self.__password = password

    def auth(self, user=None, password=None):
        """Authenticate against the provided SAML Identity Provider."""

        if not user:
            user = self.username
        if not password:
            password = self.__password

        session = requests.Session()

        try:
            r = session.get(self.url)
            # NOTE: parameters are hardcoded for Shibboleth IDP
            data = {'j_username': user,
                    'j_password': password,
                    'submit': 'Login'}

            r = session.post(r.url, data=data)
        except Exception, e:
            raise SAMLError("failed to call SAML: " + str(e))

        saml_xml = get_saml_response(r.text)
        if not saml_xml:
            raise SAMLError("Invalid SAML response.")

        result = {}
        result["success"] = saml_xml is not None
        result["duration"] = r.elapsed.total_seconds()
        result["code"] = r.status_code
        return result


def get_saml_response(html):
    """
    Parse SAMLResponse from Shibboleth page

    >>> get_saml_response('<input name="a"/>')

    >>> get_saml_response('<body xmlns="bla"><form><input name="SAMLResponse" value="eG1s"/></form></body>')
    u'xml'
    """
    soup = BeautifulSoup(html, "html.parser")

    for elem in soup.find_all('input', attrs={'name': 'SAMLResponse'}):
        saml_base64 = elem.get('value')
        xml = codecs.decode(saml_base64.encode('ascii'), 'base64').decode('utf-8')
        return xml


if __name__ == '__main__':
    import sys
    import logging
    from zmon_worker_monitor import plugin_manager

    logging.basicConfig(level=logging.INFO)

    # init plugin manager and collect plugins, as done by Zmon when worker is starting
    plugin_manager.init_plugin_manager()
    plugin_manager.collect_plugins(load_builtins=True, load_env=True)

    saml_url = sys.argv[1]
    factory_ctx = {}

    saml = SAMLWrapper(saml_url, user=os.getenv("SAML_USERNAME"), password=os.getenv("SAML_PASSWORD")).auth()
    print(saml)
