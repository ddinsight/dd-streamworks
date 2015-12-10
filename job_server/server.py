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


import uuid
import time
import gevent
from gevent import Greenlet
from urlparse import urlparse
import sched
import cherrypy
import pymongo
import json
import settings
from settings import conf as config
from msgfeeder import MessageFeeder
import scubagear.logger as log
import kazoo
from kazoo.client import KazooClient
from kazoo.protocol.states import KazooState
import socket
from collections import Counter
from kazoo.handlers.gevent import SequentialGeventHandler
from collections import defaultdict


class ZKNode(object):
    version = -1
    cversion = -1

    def __init__(self, version=-1, cversion=-1):
        self.version = version
        self.cversion = cversion
        self._data = {}

    @property
    def data(self):
        return self._data

    @data.setter
    def data(self, d):
        if isinstance(d, (str, unicode)):
            d = json.loads(d)
        self._data = d


class ZKNodeGroup(ZKNode):
    def __init__(self):
        super(ZKNodeGroup, self).__init__()
        self.members = {}


class HenemServer(Greenlet):
    def __init__(self):
        Greenlet.__init__(self)
        self.uid = str(uuid.uuid4())
        self.beginTime = time.time()
        self.feeders = []
        self.running = False
        self.scheduler = sched.scheduler(time.time, self.__schedDelay)

        self.workerNodes = []
        self.subscribedEvents = dict()

        self.mongoClient = None
        self.rootpath = '/'
        self.taskDB = ZKNodeGroup()
        self.workerDB = ZKNodeGroup()
        # configure OceanKeeper
        self.metaClient = self.__setupMetaServerConnection()

    def __del__(self):
        if self.mongoClient:
            self.mongoClient.close()

    def __schedDelay(self, delay):
        """
        Delay function for main scheduler

        :param delay: delay time in seconds
        :return: None
        """
        delayDelta = 1
        while self.running and delay > 0:
            gevent.sleep(delayDelta)
            delay -= delayDelta

    def __setupMetaServerConnection(self):
        keeperHosts = config.get('server', 'meta')
        parsed = urlparse(keeperHosts)

        if parsed.scheme != 'zk':
            raise ValueError("Meta URL must start with zk://")

        if parsed.path in ('/', ''):
            raise ValueError("Service root path not found.")

        self.rootpath = parsed.path.rstrip('/')

        # NOTE: currently, auth_data is not supported
        servers = parsed.netloc.split('@')[-1]
        metaClient = KazooClient(hosts=servers, handler=SequentialGeventHandler())
        metaClient.add_listener(self.__connection)

        return metaClient

    def __checkMetaInfo(self):
        if not self.metaClient.connected:
            raise RuntimeError("Zookeeper server is not connected")

        metaEntryList = ('', '/henem/config', '/henem/host', '/oceanworker/config', '/oceanworker/tasks', '/oceanworker/workers', '/oceanworker/event/worker')

        for entry in metaEntryList:
            if not self.metaClient.exists(self.rootpath + entry):
                self.metaClient.create(self.rootpath + entry, makepath=True)

    def __setupMetaEventCallback(self):
        try:
            self.metaClient.DataWatch(self.rootpath + '/oceanworker/tasks', self.__eventTask)
            self.metaClient.ChildrenWatch(self.rootpath + '/oceanworker/workers', self.__eventWorker, send_event=True)
        except Exception, e:
            log.error(e)

    def __reconnect(self):
        gevent.sleep(1)
        try:
            self.metaClient.restart()
        except (kazoo.exceptions.SessionExpiredError, kazoo.exceptions.ConnectionLoss, kazoo.exceptions.ConnectionClosedError) as detail:
            gevent.spawn(self.__reconnect)
        except Exception, e:
            log.error(e)
            gevent.spawn(self.__reconnect)

    def __connection(self, state):
        log.error(state)
        if state == KazooState.CONNECTED:
            gevent.spawn(self.__setupMetaEventCallback)

    def __eventTask(self, data, event):
        try:
            (nodes, stat) = self.metaClient.get_children(self.rootpath + '/oceanworker/tasks', include_data=True)
            self.taskDB.version = stat.version
            self.taskDB.cversion = stat.cversion
            self.taskDB.tasks = {}

            for node in nodes:
                (data, stat) = self.metaClient.get(self.rootpath + '/oceanworker/tasks/%s' % node)
                znode = ZKNode(stat.version, stat.cversion)
                znode.data = data
                if znode.data.get('maxWorker', 0) > 0:
                    self.taskDB.members[node] = znode

            for feeder in self.feeders:
                feeder.updateSubscribeEvents(self.getEventSubscribeStatus())

        except (kazoo.exceptions.SessionExpiredError, kazoo.exceptions.ConnectionLoss, kazoo.exceptions.ConnectionClosedError) as detail:
            gevent.spawn(self._reconnect)
        except Exception, e:
            log.error(e)

    def __eventWorker(self, data, event=None):
        try:
            if isinstance(data, (list, tuple)):
                log.info("NODES : %d" % len(data))

            self.__updateWorkerStatus()
        except (kazoo.exceptions.SessionExpiredError, kazoo.exceptions.ConnectionLoss, kazoo.exceptions.ConnectionClosedError) as detail:
            gevent.spawn(self.__reconnect)
        except Exception, e:
            log.error(e)

    def __updateWorkerStatus(self):
        try:
            (nodes, stat) = self.metaClient.get_children(self.rootpath + '/oceanworker/workers', include_data=True)

            if self.workerDB.version == stat.version and self.workerDB.cversion == stat.cversion:
                return

            self.workerDB.version = stat.version
            self.workerDB.cversion = stat.cversion
            self.workerDB.members = {}

            for node in nodes:
                try:
                    (data, stat) = self.metaClient.get(self.rootpath + '/oceanworker/workers/%s' % node)
                    znode = ZKNode(stat.version, stat.cversion)
                    znode.data = data

                    self.workerDB.members[node] = znode
                except Exception, e:
                    log.error(e)
                    continue

            for feeder in self.feeders:
                feeder.updateWorkerStatus(self.getTaskAssignStatus())
        except (kazoo.exceptions.SessionExpiredError, kazoo.exceptions.ConnectionLoss, kazoo.exceptions.ConnectionClosedError) as detail:
            gevent.spawn(self.__reconnect)
        except Exception, e:
            log.error(e)

    def getEventSubscribeStatus(self):
        #return map(lambda x: zip(x.data.get('subscribeEvents'), [x.data.get('hashKey')] * len(x.data.get('subscribeEvents'))), self.taskDB.members.values())
        """
            extract subscribed event list with hashKey of task and flatten,
            WARNING!!!! : CANNOT assign multiple hashKey to same event
            ex)
                task1 : subscribeEvents -> [event1, event2], hashKey -> None
                task2 : subscribeEvents -> [event2, event3], hashKey -> None
                task3 : subscribeEvents -> [event1], hashKey -> deviceID

                result -> {event1: deviceID, event2: None, event3: None}
        """

        # extract (event, hashKey) tuple lists
        events = [event for znode in self.taskDB.members.values() for event in zip(znode.data.get('subscribeEvents'), [znode.data.get('hashKey')] * len(znode.data.get('subscribeEvents')))]

        if len(events) == 0:
            return {}

        subscribeInfo = defaultdict(None).fromkeys(zip(*events)[0])
        for e, hk in filter(lambda x: x[1] != None, events):
            subscribeInfo[e] = hk

        return subscribeInfo

    def getTaskAssignStatus(self):
        tasks = [task for znode in self.workerDB.members.values() for task in znode.data.get('tasks', []) + znode.data.get('devTasks', [])]
        return dict(Counter(tasks))

    def stop(self):
        log.info("stop maintenance thread..")

        self.running = False
        for evt in self.scheduler.queue:
            self.scheduler.cancel(evt)

    def __checkMongo(self):
        try:
            mongoqueue = self.db['APATLogQueue'].count()
            log.debug("QUEUE - %d" % mongoqueue)
        except pymongo.errors.AutoReconnect:
            pass
        except Exception, e:
            log.error(e)

        self.scheduler.enter(10, 1, self.__checkMongo, ())

    def _run(self):
        log.info("start maintenance thread..")

        # connecting zookeeper
        try:
            self.metaClient.start()
            self.__checkMetaInfo()
        except Exception, e:
            log.error(e)
            raise e

        try:
            svrConfig = json.loads(self.metaClient.get(self.rootpath + '/henem/config')[0])
            log.info("Config - %s" % json.dumps(svrConfig))
            config.update(svrConfig, keepOrgValue=True)
        except Exception, e:
            log.error(e)

        log.info("Service Configuration from local and Zookeeper")
        log.info(config.dumps())

        self.urlQueue = config.get('source', 'mgqueue')
        try:
            self.mongoClient = pymongo.MongoClient(self.urlQueue)
            self.db = self.mongoClient.get_default_database()
        except Exception, e:
            log.error(self.urlQueue)
            log.error(e)
            raise e

        self.keeperInfo = dict(name='henem')
        self.keeperInfo['host'] = "%s:%s" % (socket.gethostbyname(socket.gethostname()), config.get('main', 'port'))

        self.feeders.append(MessageFeeder(self.db['APATLogQueue'], ordering=False))

        self.running = True

        for feeder in self.feeders:
            feeder.updateSubscribeEvents(self.getEventSubscribeStatus())
            feeder.updateWorkerStatus(self.getTaskAssignStatus())
            feeder.start()

        self.scheduler.enter(10, 1, self.__checkMongo, ())

        try:
            self.scheduler.run()
        except Exception, e:
            log.error(e)
            pass

        for feeder in self.feeders:
            feeder.stop()

        self.metaClient.stop()
        self.metaClient.close()
        self.mongoClient.close()

        log.info("Exit Server Thread")

    @cherrypy.expose(alias=['id', 'ping'])
    def getId(self):
        return self.uid

