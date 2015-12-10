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


try:
    import scubagear.logger as log
except ImportError:
    import logging as log

from kombu import Connection, Exchange
from kombu.pools import producers, connections

from connection import AMQPConnectionFactory


class AMQPProducer(object):
    def __init__(self, url):
        self.url = url
        self.connection = connections[Connection(self.url)].acquire()

    def start(self):
        self.connection.connect()

    def stop(self):
        self.connection.release()

    def onError(self, exc, interval):
        log.error("PUBLISH ERROR - %s" % exc)

    def publish(self, exchange, key, data, arguments=None):
        if not self.connection.connected:
            return False

        try:
            with producers[self.connection].acquire(block=True) as producer:
                ex = Exchange(exchange, type='direct')
                self.connection.ensure(producer, producer.publish, errback=self.onError, max_retries=10, interval_max=3)
                producer.publish(data,
                                 routing_key=key,
                                 delivery_mode=2,
                                 compression='bzip2',
                                 headers=arguments,
                                 exchange=ex,
                                 retry=True)
                return True
        except Exception, e:
            log.error(e)
        return False
