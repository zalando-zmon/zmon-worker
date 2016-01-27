#!/usr/bin/env python
# -*- coding: utf-8 -*-

from dogpile.cache import make_region
import threading

DEFAULT_CACHE_EXPIRATION_TIME = 3600


def async_creation_runner(cache, somekey, creator, mutex):
    ''' Used by dogpile.core:Lock when appropriate '''

    def runner():
        try:
            value = creator()
            cache.set(somekey, value)
        finally:
            mutex.release()

    thread = threading.Thread(target=runner)
    thread.start()


# Asynchronous cache decorator persisted to memory.
# After the first successful invocation cache updates happen in a background thread.
async_memory_cache = make_region(async_creation_runner=async_creation_runner).configure('dogpile.cache.memory',
        expiration_time=DEFAULT_CACHE_EXPIRATION_TIME)
