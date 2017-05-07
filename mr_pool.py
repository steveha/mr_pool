#!/usr/bin/env python
# _*_ coding: utf-8

"""
mr_pool -- "Mister Pool", Map/Reduce Pool

"""

import functools
import logging
import multiprocessing as mp


VERSION = (0, 8, 0)

CPU_COUNT = mp.cpu_count()

class LogToList(object):
    def __init__(self, level=logging.INFO):
        self.log_level = level
        self.lst = []
    def _log(self, message, level=logging.INFO):
        if level < self.log_level:
            return
        tup = (level, message)
        self.lst.append(tup)
    def debug(self, message):
        self._log(message, level=logging.DEBUG)
    def info(self, message):
        self._log(message, level=logging.INFO)
    def warn(self, message):
        self._log(message, level=logging.WARNING)
    def warning(self, message):
        self._log(message, level=logging.WARNING)
    def error(self, message):
        self._log(message, level=logging.ERROR)
    def critical(self, message):
        self._log(message, level=logging.CRITICAL)


class MRPoolError(Exception):
    pass

MR_POOL_NONE = object()  # unique object used by run(), below

def base_worker(fn, work):
    # function that wraps the map function to catch exceptions
    log = LogToList()
    exc = None
    try:
        result = fn(work)
    except Exception as exc:
        result = None
    return (work, result, exc, log.lst)

def MRPool(fn_map, fn_reduce,
        num_workers=None,
        stop_on_exception=False,
        pool_log=None,
        log_level=logging.INFO):

    # This makes a new function that binds together base_worker() with the
    # provided fn_map() function.  We need a single function that just takes
    # one argument, the data to be processed.  And, because of implementation
    # details of Pool().map(), we cannot use a class instance, has to be a
    # plain function.  (Otherwise you get an error on "pickling".)
    worker = functools.partial(base_worker, fn_map)

    def pool_reduce(accumulator, worker_tup):
        # function that wraps the map function to catch exceptions
        work, result, exc, lst_log = worker_tup

        if exc is not None and stop_on_exception:
            x = MRPoolError("MRPool early exit for exception")
            x.work, x.result, x.exc = worker_tup
            raise x

        if pool_log is not None:
            # write out all saved log messages to provided log
            for tup in lst_log:
                level, message = tup
                pool_log.log(level, message)
            if exc is not None:
                pool_log.error("MRPool caught exception", {"work":repr(work)[:255], "exc": repr(exc)[:255]})

        if exc is not None:
            return
        if accumulator is MR_POOL_NONE:
            accumulator = result
        else:
            accumulator = fn_reduce(accumulator, result)
        return accumulator

    def run(work_to_do, initial=MR_POOL_NONE ):
        accumulator = initial
        if num_workers == 1:
            # Cannot single-step inside Pool() but can single-step inside this loop.
            # When you want to run debugger just run with num_workers=1
            for work in work_to_do:
                tup = worker(work)
                accumulator = pool_reduce(accumulator, tup)
        else:
            p = mp.Pool(processes=num_workers)
            for tup in p.imap(worker, work_to_do):
                accumulator = pool_reduce(accumulator, tup)
        return accumulator
    return run
