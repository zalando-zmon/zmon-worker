#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime
import json
import numpy

from collections import Set
from decimal import Decimal


class JsonDataEncoder(json.JSONEncoder):
    def default(self, o):
        '''
        >>> JsonDataEncoder().encode(datetime.datetime.now())[:3]
        '"20'
        >>> JsonDataEncoder().encode(Decimal('3.14'))
        '3.14'
        >>> JsonDataEncoder().encode(set([1, 2]))
        '[1, 2]'
        >>> JsonDataEncoder().encode(numpy.nan)
        'null'
        >>> JsonDataEncoder().encode(numpy.Infinity)
        '"Infinity"'
        '''
        if isinstance(o, (datetime.datetime, datetime.date, datetime.time)):
            return o.isoformat()
        elif isinstance(o, Decimal):
            return float(o)
        elif isinstance(o, Set):
            return list(o)
        elif isinstance(o, numpy.bool_):
            return bool(o)
        else:
            return super(JsonDataEncoder, self).default(o)

    def iterencode(self, o, _one_shot=False):
        for chunk in super(JsonDataEncoder, self).iterencode(o, _one_shot=_one_shot):
            yield {'NaN': 'null', 'Infinity': '"Infinity"', '-Infinity': '"-Infinity"'}.get(chunk, chunk)
