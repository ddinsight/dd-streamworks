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


import MySQLdb
import pymongo
from urlparse import urlparse
import threading

try:
    import scubagear.logger as log
except ImportError:
    import logging as log

# global parameters
__dbConfigParams = {}

# for multi thread. thread-safe.
__dbConnections = None

"""
import atexit

@atexit.register
def goodbye():
    print "You are now leaving the Python sector."
"""


# External Interface
def initDbParams(params):
    global __dbConfigParams, __dbConnections

    # Create thread-local
    __dbConnections = threading.local()

    # url parsing for DB Connection params
    for name, url in params.iteritems():
        __dbConfigParams[name] = url


# MySQL External IF
def allocDictCursor(name):
    db = __getMySQLdb__(name)
    if not db:
        log.error("Cannot find DB connection info - %s" % name)
        return None

    return db.cursor(MySQLdb.cursors.DictCursor)


def allocCursor(name):
    db = __getMySQLdb__(name)
    if not db:
        return None

    return db.cursor()


def freeCursor(cursor):
    #print "freeCursor", cursor.connection
    cursor.connection.commit()
    cursor.close()


# MongoDB External IF
def allocMongoHandler(name):
    return __getMongodb__(name)


def freeMongoHandler(handler):
    # do nothing
    pass


# Internal IF
def __getMySQLdb__(name):
    def connect(name):
        global __dbConfigParams

        if not __dbConfigParams.has_key(name):
            raise ValueError("invalid db connection name - %s" % name)

        try:
            url = urlparse(__dbConfigParams[name])
        except Exception, e:
            raise ValueError("db url parsing error - %s : %s" % (name, __dbConfigParams[name]))

        connParams = dict(charset='utf8')

        connParams['db'] = url.path.strip('/')
        connParams['host'] = url.hostname

        if url.port:
            connParams['port'] = url.port

        if url.username:
            connParams['user'] = url.username

        if url.password:
            connParams['passwd'] = url.password

        db = MySQLdb.connect(**connParams)
        db.autocommit(True)

        return db

    global __dbConnections

    db = getattr(__dbConnections, name, None)
    if not db:
        try:
            db = connect(name)
        except Exception,e:
            log.error("MySQL Connect Error - %s" % e)
            return None
        setattr(__dbConnections, name, db)

    db.ping(True)

    return db


# MongoDB functions
def __getMongodb__(name):
    def connect(name):
        global __dbConfigParams

        if not __dbConfigParams.has_key(name):
            raise ValueError("invalid db connection name - %s" % name)

        url = __dbConfigParams[name]
        client = pymongo.MongoClient(url)

        return client

    conn = getattr(__dbConnections, name, None)
    if not conn:
        try:
            conn = connect(name)
        except Exception,e:
            log.error("MongoDB Connect Error - %s" % e)
            return None
        setattr(__dbConnections, name, conn)

    return conn.get_default_database()

