#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import requests
import sys
import urllib
import urlparse
import json
from prometheus_client.parser import text_string_to_metric_families
from collections import defaultdict

from zmon_worker_monitor.zmon_worker.errors import HttpError, CheckError, ConfigurationError
from zmon_worker_monitor.zmon_worker.common.http import get_user_agent
from requests.adapters import HTTPAdapter

from zmon_worker_monitor.adapters.ifunctionfactory_plugin import IFunctionFactoryPlugin, propartial

import tokens

logger = logging.getLogger('zmon-worker.http-function')

ACTUATOR_METRIC_NAMES = {'p99': '99th', 'p75': '75th', 'p50': 'median', 'm1_rate': 'mRate', '99%': '99th',
                         '75%': '75th', '1m.rate': 'mRate', 'count': 'count', 'oneMinuteRate': 'mRate',
                         'min': 'min', 'max': 'max', 'mean': 'mean', 'median': 'median', '75thPercentile': '75th',
                         '99thPercentile': '99th'}


class HttpFactory(IFunctionFactoryPlugin):
    def __init__(self):
        super(HttpFactory, self).__init__()

    def configure(self, conf):
        """
        Called after plugin is loaded to pass the [configuration] section in their plugin info file
        :param conf: configuration dictionary
        """
        # will use OAUTH2_ACCESS_TOKEN_URL environment variable by default
        # will try to read application credentials from CREDENTIALS_DIR
        tokens.configure()

        token_configuration = conf.get('oauth2.tokens')

        if token_configuration:
            for part in token_configuration.split(':'):
                token_name, scopes = tuple(part.split('=', 1))
                tokens.manage(token_name, scopes.split(','))

        tokens.manage('uid', ['uid'])

        tokens.start()

    def create(self, factory_ctx):
        """
        Automatically called to create the check function's object
        :param factory_ctx: (dict) names available for Function instantiation
        :return: an object that implements a check function
        """
        return propartial(HttpWrapper, base_url=factory_ctx.get('entity_url'))


def absolute_http_url(url):
    '''
    >>> absolute_http_url('')
    False

    >>> absolute_http_url('bla:8080/blub')
    False

    >>> absolute_http_url('https://www.zalando.de')
    True
    '''

    return url.startswith('http://') or url.startswith('https://')


def map_dropwizard_timers(timers, prefix):
    r = {}

    start_index = len(prefix.split('.')) - 1
    for (k, v) in timers.iteritems():
        if k.startswith(prefix):
            ks = k.split('.')
            ks = ks[start_index:]

            status_code, http_method = ks[:2]
            path = '.'.join(ks[2:])

            if path not in r:
                r[path] = {}

            if http_method not in r[path]:
                r[path][http_method] = {}

            if status_code not in r[path][http_method]:
                r[path][http_method][status_code] = {}

            for (mn, mv) in v.iteritems():
                # map Drop Wizard metric names to our canonical ones
                metric_name = ACTUATOR_METRIC_NAMES.get(mn)
                if metric_name:
                    r[path][http_method][status_code][metric_name] = mv
    return r


def map_spring_boot_metrics(j, prefix):
    r = {}
    # process "flat" Spring Boot metrics
    # see https://github.com/zalando/zmon-actuator
    start_index = len(prefix.split('.')) - 1
    for (k, v) in j.iteritems():
        if k.startswith(prefix):
            # dict key "k" looks like:
            # my.prefix.200.GET.my.foo.bar.path.oneMinuteRate
            # or
            # my.prefix.200.GET.my.foo.bar.path.snapshot.99thPercentile
            ks = k.split('.')
            # strip prefix (usually "zmon.response.")
            ks = ks[start_index:]

            # ks == ['200', 'GET', 'my', 'foo', 'bar', 'path', 'snapshot', '99thPercentile']

            if ks[-2] == 'snapshot':
                path = '.'.join(ks[2:-2])
            else:
                path = '.'.join(ks[2:-1])

            if path not in r:
                r[path] = {}

            status_code, http_method = ks[:2]
            if http_method not in r[path]:
                r[path][http_method] = {}

            if status_code not in r[path][ks[1]]:
                r[path][http_method][status_code] = {}

            # map Spring Boot metric names to our canonical ones
            metric_name = ACTUATOR_METRIC_NAMES.get(ks[-1])
            if metric_name:
                r[path][http_method][status_code][metric_name] = v

    return r


class HttpWrapper(object):
    def __init__(
            self,
            url,
            method='GET',
            params=None,
            base_url=None,
            timeout=10,
            max_retries=0,
            allow_redirects=None,
            verify=True,
            oauth2=False,
            oauth2_token_name='uid',
            headers=None,
    ):
        if method.lower() not in ('get', 'head'):
            raise CheckError('Invalid method. Only GET and HEAD are supported!')

        if not base_url and not absolute_http_url(url):
            # More verbose error message!
            raise ConfigurationError(
                'HTTP wrapper improperly configured. Invalid base_url. Check entity["url"] or call with absolute url.')

        self.url = (base_url + url if not absolute_http_url(url) else url)
        self.clean_url = None
        self.params = params
        self.timeout = timeout
        self.max_retries = max_retries
        self.verify = verify
        self._headers = headers or {}
        self.oauth2 = oauth2
        self.oauth2_token_name = oauth2_token_name
        self.__method = method.lower()

        self.allow_redirects = True if allow_redirects is None else allow_redirects
        if self.__method == 'head' and allow_redirects is None:
            self.allow_redirects = False

        self.__r = None

    def __request(self, raise_error=True, post_data=None):
        if self.__r is None:
            if self.max_retries:
                s = requests.Session()
                s.mount('', HTTPAdapter(max_retries=self.max_retries))
            else:
                s = requests

            base_url = self.url
            basic_auth = None

            url_parsed = urlparse.urlsplit(base_url)
            if url_parsed and url_parsed.username and url_parsed.password:
                base_url = base_url.replace(
                    "{0}:{1}@".format(urllib.quote(url_parsed.username), urllib.quote(url_parsed.password)), "")
                base_url = base_url.replace("{0}:{1}@".format(url_parsed.username, url_parsed.password), "")
                basic_auth = (url_parsed.username, url_parsed.password)
            self.clean_url = base_url

            if self.oauth2:
                self._headers.update({'Authorization': 'Bearer {}'.format(tokens.get(self.oauth2_token_name))})

            self._headers.update({'User-Agent': get_user_agent()})

            try:
                if post_data is None:
                    # GET or HEAD
                    get_method = getattr(s, self.__method)
                    self.__r = get_method(base_url, params=self.params, timeout=self.timeout, verify=self.verify,
                                          headers=self._headers, auth=basic_auth, allow_redirects=self.allow_redirects)
                else:
                    self.__r = s.post(base_url, params=self.params, timeout=self.timeout, verify=self.verify,
                                      headers=self._headers, auth=basic_auth, data=json.dumps(post_data))
            except requests.Timeout, e:
                raise HttpError('timeout', self.clean_url), None, sys.exc_info()[2]
            except requests.ConnectionError, e:
                raise HttpError('connection failed', self.clean_url), None, sys.exc_info()[2]
            except Exception, e:
                raise HttpError(str(e), self.clean_url), None, sys.exc_info()[2]
        if raise_error:
            try:
                self.__r.raise_for_status()
            except requests.HTTPError, e:
                raise HttpError(str(e), self.clean_url), None, sys.exc_info()[2]
        return self.__r

    def json(self, raise_error=True):
        r = self.__request(raise_error=raise_error)
        try:
            return r.json()
        except Exception, e:
            raise HttpError(str(e), self.url), None, sys.exc_info()[2]

    def jolokia(self, read_requests, raise_error=True):
        '''
        :param read_requests: see https://jolokia.org/reference/html/protocol.html#post-request
        :type read_requests: list
        :param raise_error:
        :return: Jolokia response
        '''
        def set_read_type(x):
            x['type'] = 'read'

        # hack quick verify
        if (not self.url.endswith('jolokia/')) or ('?' in self.url) or ('&' in self.url):
            raise HttpError("URL needs to end in jolokia/ and not contain ? and &", self.url)

        map(set_read_type, read_requests)

        r = self.__request(post_data=read_requests, raise_error=raise_error)

        try:
            return r.json()
        except Exception, e:
            raise HttpError(str(e), self.url), None, sys.exc_info()[2]

    def actuator_metrics(self, prefix='zmon.response.', raise_error=True):
        """
        /metric responds with keys like: zmon.response.<status>.<method>.<end-point>

        Response map is ep->method->status->metric
        """
        response = self.json(raise_error=raise_error)
        if not isinstance(response, dict):
            raise HttpError('Invalid actuator metrics: response must be a JSON object', self.url)

        # for clojure projects we use the dropwizard servlet, there the json looks slightly different
        if "timers" in response:
            return map_dropwizard_timers(response['timers'], prefix)
        else:
            return map_spring_boot_metrics(response, prefix)

    def text(self, raise_error=True):
        r = self.__request(raise_error=raise_error)
        return r.text

    def prometheus(self):
        t = self.__request().text
        samples_by_name = defaultdict(list)

        for l in text_string_to_metric_families(t):
            for s in l.samples:
                samples_by_name[s[0]].append((s[1], s[2]))

        return samples_by_name

    def headers(self, raise_error=True):
        return self.__request(raise_error=raise_error).headers

    def cookies(self, raise_error=True):
        return self.__request(raise_error=raise_error).cookies

    def content_size(self, raise_error=True):
        return len(self.__request(raise_error=raise_error).content)

    def time(self, raise_error=True):
        return self.__request(raise_error=raise_error).elapsed.total_seconds()

    def code(self):
        return self.__request(raise_error=False).status_code
