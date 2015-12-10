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


import traceback
import time
import os
import sys
import Queue
import importlib
from urlparse import urlparse

import meta
from settings import *
from settings import conf as workerConfig
import gevent

import signal
import task
import slot
import module
import events
import socket

import scubagear.dbmanager as dbmanager
from scubagear.pattern import Singleton
import scubagear.httpif as httpif

try:
    import scubagear.logger as log
except ImportError:
    import logging as log


class OceanWorker(object):
    __metaclass__ = Singleton

    def __init__(self, slotCount=8, environment=None):
        """
        intialize worker

        :param slotCount: maximum task slot number
        :param environment: worker environment. 'production' or 'development'
        """

        modulePath = os.path.join(os.getcwd(), DIR_TASK_MODULE)

        if not os.path.exists(modulePath):
            os.mkdir(modulePath)

        signal.signal(signal.SIGTERM, self.__signalHandler)
        signal.signal(signal.SIGINT, self.__signalHandler)

        sys.path.append(modulePath)
        sys.path.append(os.getcwd())
        sys.path.append(os.path.join(os.getcwd(), DIR_DEV_MODULE))

        self.slotCount = slotCount
        self.taskSlots = []
        self.devTaskSlots = []
        self.taskList = dict()
        self.msgQueue = Queue.Queue()

        # set excution environment
        if environment in ('production', 'development'):
            self.environment = environment
        else:
            self.environment = workerConfig.get('main', 'environment')

        # setup logging
        logPath, tmp, logFile = workerConfig.get('log', 'file').rpartition('/')
        logLevel = workerConfig.get('log', 'level') if self.environment == 'production' else 'DEBUG'
        log.setupLogger('%s/%s_%s' % (logPath, os.getpid(), logFile), logLevel, 'oceanworker')

        # join worker node
        worker = dict()
        worker['environment'] = self.environment
        worker['maxSlot'] = slotCount
        worker['tasks'] = []
        try:
            worker['ip'] = socket.gethostbyname(socket.gethostname())
        except Exception, e:
            worker['ip'] = '0.0.0.0'
        worker['lastEvtId'] = None

        self.metaClient = meta.MetaHandler(hosts=ZK_URL, nodeInfo=worker, eventCallback=self.__metaEventHandler)

        # update global (server-side) configuration
        try:
            workerConfig.update(self.metaClient.getConfig(), keepOrgValue=True)
        except Exception, e:
            log.error("Meta Configuration error - %s" % e)

        log.debug(dict(workerConfig.items('db')))

        if workerConfig.has_section('db'):
            dbmanager.initDbParams(dict(workerConfig.items('db')))

        if workerConfig.has_section('server') and workerConfig.has_option('server', 'task_module'):
            self.moduleClient = module.TaskModule(workerConfig.get('server', 'task_module'))
            if self.environment == 'production':
                self.downloadModules()

        self.taskClient = task.TaskManager(self.metaClient)

        self.slotPool = slot.SlotPool()
        self.slotPool.open()


    def __signalHandler(self, signum, frame):
        self.stop()

    def __metaEventHandler(self, event, evtObj=None):
        log.debug("Receive Meta Event - %s, %s" % (event, evtObj))
        if self.environment != 'production':
            return

        self.msgQueue.put((event, evtObj))

    def __updateWorkerTasks(self, tasks):
        newTaskSlots = []
        #currentSlotTask = self.slotPoolgetCurrentSlots()
        for task in tasks:
            #print "TASK!!!", task, newTaskSlots
            # TODO : Check module version and task name
            ret = filter(lambda x: x.taskName == task, self.taskSlots)
            if len(ret) > 0:
                self.taskSlots.remove(ret[0])
                newTaskSlots.append(ret[0])
            else:
                moduleName = self.taskList.get(task)
                if moduleName == None:
                    log.error("Task not found.. - %s" %self.taskList)
                    continue

                if moduleName not in sys.modules:
                    module = importlib.import_module(moduleName)
                else:
                    module = sys.modules[moduleName]

                handler = getattr(module, task)
                slot = self.slotPool.requestSlot(task, handler)
                newTaskSlots.append(slot)

        map(lambda x: self.slotPool.releaseSlot(x), self.taskSlots)
        self.taskSlots = newTaskSlots

    def __loadDevTask(self):
        info = self.metaClient.getNodeInfo()
        info['devTasks'] = []
        info['environment'] = self.environment

        for name in os.listdir(os.path.join(os.getcwd(), DIR_DEV_MODULE)):
            if not os.path.isdir(os.path.join(os.getcwd(), DIR_DEV_MODULE, name)):
                continue
            try:
                module = importlib.import_module(name)
                for e in dir(module):
                    attr = getattr(module, e)
                    if hasattr(attr, 'OW_TASK_SUBSCRIBE_EVENTS'):
                        log.info("Load Task - %s" % attr.__name__)
                        retryCnt = 3

                        while retryCnt > 0:
                            """
                            start slots after request complete.
                            because, AMQP communication affects zookeeper communication

                            TODO: solve this problem.
                            """
                            slot = self.slotPool.requestSlot('%s_dev' % attr.__name__, attr, auto_start=False)
                            if slot != None:
                                info['devTasks'].append(slot.taskName)
                                self.metaClient.addDevTask(slot.taskName, slot.task)
                                self.devTaskSlots.append(slot)
                                break
                            else:
                                retryCnt -= 1
                                gevent.sleep(1)

            except ImportError, e:
                log.error("Import ERROR - [%s] %s" % (name, e))

        self.metaClient.setNodeInfo(info)

        jobs = [gevent.spawn(slot.start) for slot in self.devTaskSlots]
        gevent.joinall(jobs, timeout=5)

    def downloadModules(self):
        self.moduleClient.open()

        self.moduleClient.cleanup()

        modules = self.metaClient.getModuleList()
        self.taskList = dict()

        for name in modules:
            # TODO : add module version to taskList
            self.moduleClient.get(name)
            info = self.metaClient.getModuleInfo(name)
            self.taskList.update(dict(map(lambda x: (x, name), info['tasks'])))

        self.moduleClient.close()

    def start(self):
        log.info("Start Worker [%s] - %s" % (id(self), time.time()))
        self.bLoop = True
        self.metaClient.open()

        if self.environment != 'production':
            try:
                self.__loadDevTask()
            except Exception ,e:
                log.error(e)

        while self.bLoop:
            try:
                (event, evtObj) = self.msgQueue.get(block=False)
            except Queue.Empty:
                #log.debug("EMPTY!")
                self.metaClient.ping()
                gevent.sleep(1)
                continue
            except Exception, e:
                log.error(e)
            except KeyboardInterrupt:
                break

            try:
                if event == events.evtModuleUpdate:
                    if self.moduleClient:
                        log.info("Task module updated, Process downloading.. - %s" % evtObj)
                        self.moduleClient.open()
                        self.moduleClient.get(evtObj)
                        info = self.metaClient.getModuleInfo(evtObj)
                        self.taskList.update(dict(map(lambda x: (x, evtObj), info['tasks'])))
                        self.moduleClient.close()

                        module = sys.modules[evtObj]

                        taskHandlers = map(lambda x: getattr(module, x), info['tasks'])
                        self.slotPool.reloadSlotTaskFromModule(taskHandlers)

                elif event == events.evtModuleRemove:
                    if evtObj in self.moduleClient.getModuleList():
                        log.info("Task module removed, Process remove.. - %s" % evtObj)
                        removedTasks = filter(lambda x: self.taskList[x] == evtObj, self.taskList.keys())

                        if len(removedTasks) > 0:
                            updatedTaskList = filter(lambda name: name not in removedTasks, map(lambda x: x.taskName, self.taskSlots))
                            self.__updateWorkerTasks(updatedTaskList)

                    self.moduleClient.remove(evtObj)
                elif event == events.evtTaskRebalance:
                    self.taskClient.rebalance(evtObj['tasks'])
                elif event == events.evtWorkerUpdateTask:
                    log.debug("evtWorkerUpdateTask - %s (%s)" % (evtObj, time.time()))
                    newTask = evtObj

                    self.__updateWorkerTasks(newTask)

                    info = self.metaClient.getNodeInfo()
                    info['tasks'] = newTask
                    self.metaClient.setNodeInfo(info)
                else:
                    log.error("Unknown event - '%s' : %s" % (event, evtObj))
            except Exception, e:
                log.error("Worker Event handling exception - %s" % e)
                log.error("\n".join(traceback.format_exception(*sys.exc_info())))
                log.error("Event info - %s - %s" % (event, evtObj))

            self.msgQueue.task_done()

        # De-initialize
        log.info("Start De-initialize....")

        if len(self.devTaskSlots) > 0:
            jobs = [gevent.spawn(self.slotPool.releaseSlot, slot) for slot in self.devTaskSlots]
            gevent.joinall(jobs, timeout=5)

        if len(self.taskSlots) > 0:
            jobs = [gevent.spawn(self.slotPool.releaseSlot, slot) for slot in self.taskSlots]
            gevent.joinall(jobs, timeout=5)

        self.slotPool.close()
        self.metaClient.close()
        log.info("Exit Worker [%s] - %s" % (id(self), time.time()))

    def stop(self):
        self.bLoop = False


def getConfig(section, name):
    return workerConfig.get(section, name)


def getEnvironment():
    return OceanWorker().environment


def stop():
    OceanWorker().stop()