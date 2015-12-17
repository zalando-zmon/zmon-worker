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

from zmon_worker_monitor.zmon_worker.errors import HttpError
from requests.adapters import HTTPAdapter

from zmon_worker_monitor.adapters.ifunctionfactory_plugin import IFunctionFactoryPlugin, propartial

import tokens

# will use OAUTH2_ACCESS_TOKEN_URL environment variable by default
# will try to read application credentials from CREDENTIALS_DIR
tokens.configure()
tokens.manage('uid', ['uid'])
tokens.start()

logger = logging.getLogger('zmon-worker.http-function')

class HttpFactory(IFunctionFactoryPlugin):

    def __init__(self):
        super(HttpFactory, self).__init__()

    def configure(self, conf):
        """
        Called after plugin is loaded to pass the [configuration] section in their plugin info file
        :param conf: configuration dictionary
        """
        return

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


class HttpWrapper(object):

    def __init__(
        self,
        url,
        params=None,
        base_url=None,
        timeout=10,
        max_retries=0,
        verify=True,
        oauth2=False,
        headers=None,
    ):

        self.url = (base_url + url if not absolute_http_url(url) else url)
        self.clean_url = None
        self.params = params
        self.timeout = timeout
        self.max_retries = max_retries
        self.verify = verify
        self.headers = headers or {}
        self.oauth2 = oauth2
        self.__r = None

    def __request(self, raise_error=True, post_data = None):
        if self.__r is not None:
            return self.__r
        if self.max_retries:
            s = requests.Session()
            s.mount('', HTTPAdapter(max_retries=self.max_retries))
        else:
            s = requests

        base_url = self.url
        basic_auth = None

        url_parsed = urlparse.urlsplit(base_url)
        if url_parsed and url_parsed.username and url_parsed.password:
            base_url = base_url.replace("{0}:{1}@".format(urllib.quote(url_parsed.username), urllib.quote(url_parsed.password)), "")
            base_url = base_url.replace("{0}:{1}@".format(url_parsed.username, url_parsed.password), "")
            basic_auth = requests.auth.HTTPBasicAuth(url_parsed.username, url_parsed.password)
        self.clean_url = base_url

        if self.oauth2:
            self.headers.update({'Authorization':'Bearer {}'.format(tokens.get('uid'))})

        try:
            if post_data is None:
                self.__r = s.get(base_url, params=self.params, timeout=self.timeout, verify=self.verify,
                                 headers=self.headers, auth = basic_auth)
            else:
                self.__r = s.post(base_url, params=self.params, timeout=self.timeout, verify=self.verify,
                                  headers=self.headers, auth = basic_auth, data=json.dumps(post_data))
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

        def set_read_type(x):
            x['type'] = 'READ'

        # hack quick verify
        if (not self.url.endswith('jolokia/')) or ('?' in self.url) or ('&' in self.url):
            raise HttpError("URL needs to end in jolokia/ and not contain ? and &", self.url)

        map(set_read_type, read_requests)

        r = self.__request(post_data=read_requests, raise_error=raise_error)

        try:
            return r.json()
        except Exception, e:
            raise HttpError(str(e), self.url), None, sys.exc_info()[2]


    def actuator_metrics(self, prefix = 'zmon.response.', raise_error = True):
        """
        /metric responds with keys like: zmon.response.<status>.<method>.<end-point>

        Response map is ep->method->status->metric
        """
        j = self.json(raise_error=raise_error)
        r={}

        # for clojure projects we use the dropwizard servlet, there the json looks slightly different
        if "timers" in j:
            metric_map = {'p99':'99th','p75':'75th','mean':'median','m1_rate':'mRate','99%':'99th','75%':'75th','1m.rate':'mRate'}
            j = j["timers"]
            j["zmon.response.200.GET.metrics"]={"mRate": 0.12345}

            start_index = len(prefix.split('.')) - 1

            for (k,v) in j.iteritems():
                if k.startswith(prefix):
                    ks = k.split('.')
                    ks = ks[start_index:]

                    status = ks[0]
                    method = ks[1]
                    ep = '.'.join(ks[2:])

                    if not ep in r:
                        r[ep]={}

                    if not method in r[ep]:
                        r[ep][method]={}

                    if not status in r[ep][method]:
                        r[ep][method][status]={}

                    for (mn, mv) in v.iteritems():
                        if mn in ['count','p99','p75','m1_rate','min','max','mean','75%','99%','1m.rate','median']:
                            if mn in metric_map:
                                mn = metric_map[mn]
                            r[ep][method][status][mn]=mv
            return r

        j["zmon.response.200.GET.metrics.oneMinuteRate"]=0.12345
        for (k,v) in j.iteritems():
            if k.startswith(prefix):
                ks = k.split('.')

                if ks[-2]=='snapshot':
                    ep = '.'.join(ks[4:-2])
                else:
                    ep = '.'.join(ks[4:-1])

                if not ep in r:
                    r[ep] = {}

                # zmon.response. 200 . GET . EP .

                if ks[3] not in r[ep]:
                    r[ep][ks[3]] = {}

                if ks[2] not in r[ep][ks[3]]:
                    r[ep][ks[3]][ks[2]] = {}

                if not (ks[-2] == 'snapshot'):
                    if ks[-1] == 'count':
                        r[ep][ks[3]][ks[2]]['count']=v
                    if ks[-1] == 'oneMinuteRate':
                        r[ep][ks[3]][ks[2]]['mRate']=v
                else:
                    if ks[-1] in ['75thPercentile','99thPercentile','min','max','median']:
                        r[ep][ks[3]][ks[2]][ks[-1].replace("Percentile", "")] = v

        return r

    def text(self, raise_error=True):
        r = self.__request(raise_error=raise_error)
        return r.text

    def prometheus(self):
        t = self.__request().text
        samples_by_name = defaultdict(list)

        for l in text_string_to_metric_families(t):
            for s in l.samples:
                samples_by_name[s[0]].append((s[1],s[2]))

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


if __name__ == '__main__':
    http = HttpWrapper(sys.argv[1], max_retries=3)
    print http.text()
