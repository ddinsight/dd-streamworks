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


import gevent
import kombu
from kombu import Connection, Exchange, Queue, Consumer
from kombu.pools import connections
from kombu.mixins import ConsumerMixin
import time

try:
    import scubagear.logger as log
except ImportError:
    import logging as log


class AMQPConsumer(ConsumerMixin):
    def __init__(self, url, callback):
        self.url = url
        self.connection = connections[Connection(self.url)].acquire()
        log.info("Connection POOL - %s" % self.connection)
        self.exchanges = []
        self.callback = callback
        self.msgCnt = 0
        self.lastSleep = 0
        
    def on_iteration(self):
        self.msgCnt += 1
        if self.msgCnt > 500 or time.time() - self.lastSleep > 1:
            gevent.sleep(0.1)
            self.msgCnt = 0
            self.lastSleep = time.time()
        pass

    def get_consumers(self, Consumer, channel):
        log.info('get_consumers')
        consumerQueues = []
        #print len(self.exchanges)
        #print dir(channel)
        #print self.connection, channel.connection

        # if ex == 'consistent-hashing' : bind 'job.general' and ex with route key
        # if ex != 'consistent-hashing' : bind ex and q with route key
        try:
            for exchange in self.exchanges:
                ex = exchange['exchange'](channel)
                ex.declare()
                if exchange['exchange'].type == 'x-consistent-hash':
                    ex.bind_to('job.general', exchange['key'])

                # TODO: update!
                # setup queue
                q = exchange['queue']
                consumerQueues.append(q)
        except Exception, e:
            log.error(e)
        return [Consumer(consumerQueues, callbacks=[self.onMessage])]

    def onMessage(self, body, message):
        if self.callback:
            try:
                self.callback(body)
                message.ack()
            except Exception, e:
                log.error(e)
        #message.nack()

    def setupExchange(self, exchange, exchangeType, queue=None, routingKey=None, weight=None, ephemeral=False):
        log.info("setupExchange - %s, %s" % (exchange, exchangeType))
        exParams = dict()
        qParams = dict()

        # exchange common setting
        exParams['name'] = exchange
        exParams['type'] = exchangeType
        exParams['delivery_mode'] = 2

        if exchangeType == "x-consistent-hash":
            exParams['durable'] = True
            exParams['internal'] = True
            exParams['arguments'] = {"hash-header": "hashID"}
            exParams['auto_delete'] = True
        else:
            exParams['durable'] = True
            exParams['internal'] = False
            exParams['auto_delete'] = False

        ex = Exchange(**exParams)

        # setup queue
        qParams['name'] = queue if queue else ''
        qParams['exchange'] = ex

        if exchangeType == "x-consistent-hash":
            qParams['routing_key'] = str(weight)
            qParams['auto_delete'] = True
            qParams['exclusive'] = True
        else:
            qParams['routing_key'] = routingKey
            qParams['auto_delete'] = False
            qParams['exclusive'] = False

        if ephemeral:
            qParams['auto_delete'] = True

        q = Queue(**qParams)

        self.exchanges.append({'queue': q, 'exchange': ex, 'key': routingKey})

    def start(self):
        self.run()

    def stop(self):
        pass

