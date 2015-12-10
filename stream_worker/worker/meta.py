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


import sys
import os
import time
import json
from urlparse import urlparse
import kazoo
from kazoo.client import KazooClient
from kazoo.protocol.states import KazooState
import settings
import events
import gevent
import traceback

try:
    import scubagear.logger as log
except ImportError:
    import logging as log


class MetaHandler(object):
    def __init__(self, hosts, nodeInfo, eventCallback):
        parsed = urlparse(hosts)
        print hosts
        if parsed.scheme != 'zk':
            raise ValueError("Meta URL must start with zk://")

        if parsed.path in ('/', ''):
            raise ValueError("Service root path not found.")

        self.rootpath = parsed.path.rstrip('/') + '/oceanworker'
        log.error("ROOT = %s" % self.rootpath)

        # NOTE: currently, auth_data is not supported
        servers = parsed.netloc.split('@')[-1]
        self.masterLock = None
        self.callback = eventCallback

        self.client = KazooClient(servers)
        self.client.add_listener(self.__connectionState)
        self.lastEventUpdated = 0

        self.bRun = False
        self.nodeInfo = nodeInfo
        self.party = self.client.ShallowParty(self.rootpath + '/workers', identifier=settings.UUID.get_hex())
        self.node = self.party.node
        self.state = self.client.state
        self.devTasks = {}

        self.client.start()

    def ping(self):
        try:
            self.client.command()
        except Exception, e:
            log.error(e)

    def __cbModule(self, node, event):
        #print "MODULE EVENT", node
        if len(node) > 0 and self.callback:
            evtInfo = json.loads(node)
            if not evtInfo:
                return

            evt = events.evtModuleUpdate if evtInfo['event'] in ('create', 'update') else events.evtModuleRemove
            self.callback(evt, evtInfo['name'])

    def __cbTask(self, node, event):
        #print "TASK EVENT"
        info = json.loads(node)
        updatedNode = info['tasks']
        ret = self.client.get_children(self.rootpath + '/tasks')

    def __processEventQueue(self):
        while True:
            try:
                msg = json.loads(self.evtQueue.get())
                if not msg:
                    break
            except TypeError, e:
                break
            print "__processEventQueue", msg

            evt = msg.pop('event')
            if evt in ['evtRebalance', 'evtTaskConfUpdate']:
                self.callback(events.evtTaskRebalance, msg)
            else:
                log.warn("UNKNOWN EVENT - %s (%s)" % (evt, msg))

        self.releaseMaster()

    def __cbEventQueue(self, node, event):
        if len(node) == 0:
            return

        if self.nodeInfo['environment'] == 'production' and self.electionMaster():
            self.client.handler.spawn(self.__processEventQueue)
        else:
            #print "Lock Already acquired or not acquire"
            pass

    def __cbEventWorker(self, node, event):
        if not node:
            return

        #print "NODE EVT", node
        evtNode = json.loads(node)
        if evtNode['event'] == 'evtWorkerUpdateTask':
            self.callback(events.evtWorkerUpdateTask, evtNode['tasks'])

    def __setupNode(self):
        try:
            if self.client.exists(os.path.join(self.party.path, self.party.node)):
                return

            self.party.data = json.dumps(self.nodeInfo)
            self.party.join()

            # create event node for worker
            nodepath = self.rootpath + '/event/worker/' + self.node
            log.error("Create worker node - %s" % nodepath)

            if not self.client.exists(nodepath):
                self.client.create(nodepath, makepath=True, ephemeral=True)

            # collect info
            try:
                eventInfo = json.loads(self.client.get(self.rootpath + '/event')[0])
                if eventInfo:
                    self.lastEventUpdated = eventInfo.get('update', time.time())
            except ValueError, e:
                pass
            except Exception, e:
                log.error(e)

            self.evtQueue = self.client.Queue(self.rootpath + '/event/queue')
            self.client.ChildrenWatch(self.rootpath + '/event/queue', func=self.__cbEventQueue, send_event=True)
            self.client.DataWatch(self.rootpath + '/module', func=self.__cbModule, send_event=True)
            self.client.DataWatch(self.rootpath + '/event/worker/' + self.node, func=self.__cbEventWorker, send_event=True)
        except Exception, e:
            ex = sys.exc_info()
            log.error(e)
            log.error(''.join(traceback.format_exception(*ex)))
            gevent.spawn(self.__reconnect)

    def __reconnect(self):
        gevent.sleep(1)
        try:
            if not self.client.exists(self.rootpath + '/worker/' + self.node):
                log.error("Reconnect - %s" % self.node)
                if len(self.devTasks) > 0:
                    for path, info in self.devTasks.iteritems():
                        self.client.create(path, json.dumps(info), ephemeral=True)
                self.__setupNode()
        except (kazoo.exceptions.SessionExpiredError, kazoo.exceptions.ConnectionLoss, kazoo.exceptions.ConnectionClosedError) as detail:
            log.error("Zookeeper : Unrecoverable Error - %s" % detail)
            # unrecoverable Error

            start = time.time()
            gevent.spawn(self.retryObj, self.client.restart)
            log.error("RECONNECT TIME - %s" % (time.time() - start))
        except Exception, e:
            # Try again
            log.error(e)
            exc_info = sys.exc_info()
            log.error(''.join(traceback.format_exception(*exc_info)))
            gevent.spawn(self.__reconnect)
            pass

    def __connectionState(self, state):
        log.error("Zookeeper Connection -> %s - %s" % (state, self.node))
        if state == KazooState.SUSPENDED:
            pass
        if state == KazooState.LOST:
            pass
        if state == KazooState.CONNECTED:
            # if connected, setup worker node
            gevent.spawn(self.__setupNode)

        self.state = state

    def open(self):
        # Check ZK tree
        if not self.client.exists(self.rootpath + '/event/queue'):
            self.client.create(self.rootpath + '/event/queue', makepath=True)
            self.client.create(self.rootpath + '/event/__masterlock', makepath=True)

        if not self.client.exists(self.rootpath + '/module'):
            self.client.create(self.rootpath + '/module', makepath=True)

        if not self.client.exists(self.rootpath + '/event/worker'):
            self.client.create(self.rootpath + '/event/worker', makepath=True)

    def close(self):
        self.party.leave()
        if self.client.connected:
            self.client.stop()
            self.client.close()

    def getModuleList(self):
        if not self.client.exists(self.rootpath + '/module'):
            return []
        return self.client.get_children(self.rootpath + '/module')

    def getTasks(self):
        taskInfo = {}
        for task in self.client.get_children(self.rootpath + '/tasks'):
            taskConf = self.client.get('%s/tasks/%s' % (self.rootpath, task))
            taskInfo[task] = json.loads(taskConf[0])

        return taskInfo

    def getConfig(self):
        return json.loads(self.client.get(self.rootpath + '/config')[0])

    def addDevTask(self, name, taskHandler):
        taskPath = '%s/tasks/%s' % (self.rootpath, name)

        if not self.client.exists(taskPath):
            taskInfo = dict()
            taskInfo['maxWorker'] = 1
            taskInfo['subscribeEvents'] = taskHandler.OW_TASK_SUBSCRIBE_EVENTS
            taskInfo['publishEvents'] = taskHandler.OW_TASK_PUBLISH_EVENTS
            taskInfo['useHashing'] = taskHandler.OW_USE_HASHING
            taskInfo['hashKey'] = taskHandler.OW_HASH_KEY
            #taskInfo['module'] = None
            print taskPath, json.dumps(taskInfo)
            self.client.create('%s/tasks/%s' % (self.rootpath, name), json.dumps(taskInfo), ephemeral=True)
            self.devTasks[taskPath] = taskInfo

    def getNodes(self):
        workerInfo = {}
        try:
            for worker in self.client.get_children(self.rootpath + '/workers'):
                info = self.client.get('%s/workers/%s' % (self.rootpath, worker))
                workerInfo[worker] = json.loads(info[0])
        except Exception, e:
            log.error("Worker Load error - worker : %s" % e)

        return workerInfo

    def getModuleInfo(self, name):
        ret = self.client.get('%s/module/%s' % (self.rootpath, name))
        return json.loads(ret[0])

    def getEvent(self):
        ret = self.evtQueue.get()
        if ret:
            return json.loads(ret)

        return None

    def electionMaster(self):
        if not self.masterLock:
            self.masterLock = self.client.Lock(self.rootpath + '/event/__masterlock', json.dumps({'worker': self.party.node, 'ts': time.time()}))

        if self.masterLock.is_acquired:
            #print "Already master."
            return False

        try:
            if self.masterLock.acquire(blocking=True, timeout=1):
                #print "ACQUIRE LOCK!"
                return True
        except Exception, e:
            #print e
            pass

        return False

    def releaseMaster(self):
        if self.masterLock and self.masterLock.is_acquired:
            #print "RELEASE!!!", self.masterLock.is_acquired
            self.masterLock.release()

    def getNodeInfo(self, node=None):
        if not node:
            node = self.node

        try:
            return json.loads(self.client.get('%s/%s' % (self.party.path, node))[0])
        except Exception, e:
            log.error(e)
            return None

    def setNodeInfo(self, info):
        self.nodeInfo = info
        try:
            self.client.set('%s/%s' % (self.party.path, self.node), json.dumps(info))
            self.client.set('%s' % (self.party.path), json.dumps({'worker': self.node, 'lastUpdate': time.time()}))
        except Exception, e:
            log.error(e)

    def sendNodeEvent(self, node, event):
        try:
            log.info("Set node info - %s, %s" % (node, event['tasks']))
            self.client.set('%s/event/worker/%s' % (self.rootpath, node), json.dumps(event))
        except Exception, e:
            log.error(e)
            return False

        return True
