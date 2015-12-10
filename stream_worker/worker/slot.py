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


from gevent import sleep, Greenlet

import time
import threading
import Queue
from functools import partial
import traceback
import sys
from types import MethodType

import worker
try:
    import scubagear.logger as log
except ImportError:
    import logging as log
import settings
from settings import conf as workerConfig
import scubagear.amqp.consumer as consumer
import scubagear.amqp.producer as producer


class SlotPool(object):
    def __init__(self, maxPool=8):
        """
        initialize slot pool

        :param maxPool: max slot number
        :return: None
        """
        self.maxPool = maxPool
        self.slots = []
        self.publishQueue = Queue.Queue()
        self.urlAMQP = None

    def open(self):
        if not workerConfig.has_section('server') or not workerConfig.has_option('server', 'broker'):
            raise EnvironmentError('Cannot find taskmodule url')
        self.urlAMQP = workerConfig.get('server', 'broker')
        self.amqpProducer = producer.AMQPProducer(url=self.urlAMQP)

    def onProducerCallback(self):
        while True:
            try:
                event, params = self.publishQueue.get(block=False)
            except Queue.Empty, e:
                sleep(0.1)
                return

            self.publishQueue.task_done()

            # non consistent-hashing only.
            try:
                self.amqpProducer.publish('job.general', event, params)
                pass
            except Exception, e:
                log.error("Publish Error - event: %s, params: %s" % (event, params))
                return

    def close(self):
        if self.amqpProducer:
            self.amqpProducer.stop()
        pass

    def requestSlot(self, name, handler, auto_start=True):
        """
        assign task slot

        :param name: task name
        :param handler: task class
        :param auto_start: if 'True', call '.start()' automatically
        :return: slot instance
        """
        log.info("REQUEST!!!!")
        slot = SlotHandler(name, handler(), self.urlAMQP, self)
        self.slots.append(slot)
        if auto_start:
            slot.start()
        return slot

    def releaseSlot(self, slot):
        """
        release assigned slot

        :param slot: slot
        :return: None
        """
        log.debug("RELEASE SLOT - %s" % slot.taskName)
        slot.stop()

    def reloadSlotTaskFromModule(self, taskHandlers=[]):
        """
        reload task of slot

        :param taskHandlers: task class list of updated module
        :return: None
        """
        taskMapper = dict(map(lambda x: (x.__name__, x), taskHandlers))

        for slot in self.slots:
            if slot.taskName in taskMapper.keys():
                # reload
                handler = taskMapper[slot.taskName]
                slot.reload(handler())

    def onAMQPConnected(self):
        #if not self.amqpProducer.channel:
        #    self.amqpProducer.start()
        pass

    def onAMQPDisconnected(self, code, message):
        #if self.amqpProducer.channel:
        #    self.amqpProducer.stop()
        pass

    def getCurrentSlots(self):
        return self.slots


class SlotHandler(Greenlet):
    def __init__(self, name, taskHandler, url, slotPool):
        """
        initialize slot

        :param name: task name
        :param taskHandler: task handler instance
        :param connector: AMQP connector
        :param slotPool: slot pool
        :return: None
        """
        Greenlet.__init__(self)
        self.slotPool = slotPool
        self.running = False
        self.reloading = False
        self.updatedTask = None
        self.taskName = name
        self.task = self.__prepareTaskHandler(taskHandler)
        self.event = taskHandler.OW_TASK_SUBSCRIBE_EVENTS
        self.useHashing = taskHandler.OW_USE_HASHING
        self.hashKey = taskHandler.OW_HASH_KEY

        self.urlAMQP = url
        self.statSumProcessTime = 0
        self.statSumProcessCount = 0
        self.statNextPeriod = time.time() + 30
        self.amqpConsumer = consumer.AMQPConsumer(self.urlAMQP, self.onConsumerCallback) if len(taskHandler.OW_TASK_SUBSCRIBE_EVENTS) > 0 else None
        self.amqpProducer = producer.AMQPProducer(self.urlAMQP) if len(taskHandler.OW_TASK_PUBLISH_EVENTS) > 0 else None

        self.devTask = True if name.endswith('_dev') else False

        log.info("Create task slot - task: %s, event: %s, useHashing: %s, hashKey: %s, AMQP id: %s)" % (name, self.event, self.useHashing, self.hashKey, id(self.amqpConsumer)))

        if self.useHashing:
            exchange = "job.consistent.%s" % name
            exchangeType = "x-consistent-hash"
            queueName = settings.UUID.get_hex()
            weight = 10
        else:
            exchange = "job.general"
            exchangeType = "direct"
            queueName = name
            weight = None

        for event in self.event:
            self.amqpConsumer.setupExchange(exchange, exchangeType, queueName, event, weight, self.devTask)

    def __prepareTaskHandler(self, handler):
        """
        prepare task handler instance. override 'publishEvent' method.

        :param handler: instance of task handler
        :return: task handler instance what is override.
        """

        def publishEvent(self, event, params, slot=None):
            #log.debug("Publish!! %s - %s" % (event, params))
            if event not in self.OW_TASK_PUBLISH_EVENTS:
                log.error("Not registered publish event from %s (event : %s)" % (self.__class__.__name__, event))
                return False

            if not slot.amqpProducer:
                log.error("No Producer!!!! from %s (event : %s)" % (self.__class__.__name__, event))
                return False

            try:
                return slot.amqpProducer.publish('job.general', event, params)
            except Exception, e:
                log.error(e)

            return False

        handler.publishEvent = partial(MethodType(publishEvent, handler, handler.__class__), slot=self)
        return handler

    def _run(self):
        self.threadId = threading.currentThread().getName()
        log.info("Start task slot - %s (%s)" % (self.taskName, self.threadId))
        self.running = True
        if self.amqpProducer:
            # Non-blocking
            self.amqpProducer.start()

        if self.amqpConsumer:
            try:
                # blocking and consume messages
                self.amqpConsumer.start()
            except KeyboardInterrupt:
                log.error("CATCH KEYBOARD INTERRUPT in SLOT LOOP!!!!!")

        log.info("Exit task slot - %s (%s)" % (self.taskName, self.threadId))

    def onConsumerCallback(self, params):
        if not self.running:
            return False

        if self.reloading:
            log.info("Replace task to updated.")
            self.task = self.updatedTask
            self.updatedTask = None
            self.reloading = False

        if params == None or len(params) == 0:
            log.error("No task params for '%s' (%s)" % (self.taskName, self.threadId))

        try:
            start = time.time()
            self.task.handler(params)
            runTime = time.time() - start
            self.statSumProcessCount += 1
            self.statSumProcessTime = runTime
            if self.statNextPeriod <= time.time():
                log.debug("%s - %d messages. Avg. %f sec" % (self.taskName, self.statSumProcessCount, 1.0 * self.statSumProcessTime / self.statSumProcessCount))
                self.statSumProcessCount = 0
                self.statSumProcessTime = 0
                self.statNextPeriod = time.time() + 30
        except Exception, e:
            log.error("Task Exception - %s (%s) - %s" % (self.taskName, e, params))
            worker.sentry.client.captureException(data=params)
            exc_type, exc_value, exc_traceback = sys.exc_info()
            log.error(traceback.format_exception(exc_type, exc_value, exc_traceback))
            return False
        except KeyboardInterrupt:
            log.warn("CATCH KEYBOARD INTERRUPT in SLOT!!!!!")
            worker.stop()
            return False

        return True

    def stop(self):
        log.debug("Get SLOT STOP! - %s" % self.taskName)
        self.running = False
        if self.amqpConsumer:
            self.amqpConsumer.stop()
        return True

    def pause(self):
        pass

    def resume(self):
        pass

    def reload(self, taskHandler):
        """
        reload slot task

        :param taskHandler: task handler instance
        :return: None
        """
        self.updatedTask = self.__prepareTaskHandler(taskHandler)
        self.reloading = True
