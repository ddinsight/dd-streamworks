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


import pymongo
from bson.objectid import *

try:
    import log
except ImportError:
    import logging as log


_flushQueue = {}


class EMPTY(Exception):
    """Exception raised by queue.fetchMessage()"""
    pass


def burstFetchMessages(collection, query=None, sort=None, skip=0, limit=0, **kwargs):
    """ Fetch message from MongoQueue

    Keyword arguments:
    collection -- MongoDB Collection object
    query -- query option (default : None)
    sort -- sort option (default :  [("_id", pymongo.ASCENDING)] )
    return cursor
    """

    try:
        cursor = collection.find(query, sort=sort, skip=skip, limit=limit)
    except Exception, e:
        log.error(e)
        raise e

    return cursor


def fetchMessage(collection, **kwargs):
    """ Fetch message from MongoQueue

    Keyword arguments:
    collection -- MongoDB Collection object
    query -- query option (default : {} )
    sort -- sort option (default :  [("_id", pymongo.ASCENDING)] )
    """
    query = kwargs.get('query', {})
    sort = kwargs.get('sort', {'_id': pymongo.ASCENDING})

    try:
        msg = collection.find_one(query=query, sort=sort)
    except Exception, e:
        log.error(e)
        raise e

    if msg is None:
        raise EMPTY

    return msg


def ackMessage(collection, msgId, flushInterval=0):
    """ ACK message to MongoQueue. remove message or set state to "COMPLETE"

    Keyword arguments:
    collection -- MongoDB Collection object
    id -- message id ("_id" field)
    remove -- whether remove or not (default : True)
    flushInterval -- flush interval, seconds (only remove mode)
    """
    try:
        if flushInterval == 0:
            msg = collection.remove({'_id': ObjectId(msgId)})
        else:
            global _flushQueue

            if not _flushQueue.has_key(collection.name):
                _flushQueue[collection.name] = {'ts': 0, 'items': []}

            flushList = _flushQueue[collection.name]
            flushList['items'].append(ObjectId(msgId))
            if (time.time() - flushList['ts']) > flushInterval:
                flushAcks(collection)
    except Exception, e:
        log.error(e)
        raise e


def flushAcks(collection):
    global _flushQueue
    log.error('FLUSH')
    flushList = _flushQueue.get(collection.name)

    if flushList and len(flushList['items']) > 0:
        collection.remove({'_id': {'$in': flushList['items']}}, multi=True)

        writeResult = collection.database.last_status()
        if writeResult['err'] is None:
            flushList['items'] = []
            flushList['ts'] = time.time()
        else:
            log.error("Mongo Operation Error - %s" % writeResult)
