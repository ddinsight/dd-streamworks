# -*- coding: utf-8 -*-
#
#  Copyright 2015 AirPlug Inc.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#


import os
import sys
import logging
import logging.handlers

COMMON_LOGFORMAT = '%(asctime)s %(process)d %(module)s %(funcName)s %(lineno)d  %(levelname)s %(message)s'

__logger = None
__formatter = logging.Formatter(COMMON_LOGFORMAT)

DEBUG = logging.DEBUG
INFO = logging.INFO
WARN = logging.WARN
ERROR = logging.ERROR
FATAL = logging.FATAL
CRITICAL = logging.CRITICAL

userLogLevel = DEFAULT_LOGLEVEL = DEBUG

# init. default logger
debug = logging.debug
info = logging.info
warn = logging.warn
error = logging.error
fatal = logging.fatal

userLogger = {}


def getLogger():
    return __logger


def isInitialized():
    return True if __logger is not None else False


def getFileLogger(name, filename=None):
    if not filename:
        raise ValueError("no filename")

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    # Set file logger
    (path, tmp, name) = filename.rpartition('/')

    try:
        os.makedirs(path, 0755)
    except Exception, e:
        pass

    while len(logger.handlers) > 0:
        logger.removeHandler(logger.handlers[0])

    handler = logging.FileHandler(filename, encoding='utf-8', delay=False)
    #
    handler.setFormatter(__formatter)
    logger.addHandler(handler)
    return logger


def setupLogger(filename=None, level=DEFAULT_LOGLEVEL, name="mainApps"):
    if __logger:
        return

    global userLogLevel

    userLogLevel = level

    try:
        __initLogger(level, name)
        if filename:
            setLogFile(filename)
    except Exception, e:
        raise e

    info("'%s' starts logging to '%s', log level is '%s'" % (name, filename, level))


def addHandler(handler, level=userLogLevel):
    global __logger, __formatter
    handler.setLevel(level)
    handler.setFormatter(__formatter)
    __logger.addHandler(handler)


def removeHandler(handler):
    global __logger
    __logger.removeHandler(handler)


def setLogFile(filename, level=userLogLevel):
    global __logger
    # Set file logger
    (path, tmp, name) = filename.rpartition('/')

    try:
        os.makedirs(path, 0755)
    except Exception, e:
        pass

    handler = logging.FileHandler(filename, encoding='utf-8', delay=False)
    addHandler(handler, level)


def __initLogger(level, name):
    """
    Initial logger
    """
    global __logger, __formatter

    # Use root logger
    logger = logging.getLogger(name)

    global info, debug, warn, error, fatal

    # Register log function
    info = logger.info
    debug = logger.debug
    warn = logger.warn
    error = logger.error
    fatal = logger.fatal

    # default log level
    logger.setLevel(level)

    while len(logger.handlers) > 0:
        logger.removeHandler(logger.handlers[0])

    try:
        __formatter = logging.Formatter(COMMON_LOGFORMAT)
    except Exception, e:
        print e

    # Set System logger
    handler = logging.StreamHandler(stream = sys.stderr)
    if handler:
        handler.setFormatter(__formatter)
        logger.addHandler(handler)

    __logger = logger

