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


from gevent import Greenlet, sleep
import time
from functools import partial

from collections import Counter
import settings
import scubagear.logger as log

from scubagear.amqp.producer import  AMQPProducer
import msgqueue
import pymongo
import datetime


# TODO: remove log type dependency, update to general message processing
LOG_TYPE_MAP = {
                    'aatlog': 'evtPlayerLog',
                    'anslog': 'evtNetworkLog',
                }

class MessageFeeder(Greenlet):
    QUEUE_WAIT_TIME = 5

    def __init__(self, collection, ordering=False):
        Greenlet.__init__(self)
        self.pause = False
        self.taskStatus = {}
        self.exchangeInfo = {}
        self.name = collection.name
        self.collection = collection
        self.doOrdering = ordering

        log.info("Fetch Message from MongoQueue - Collection : %s" % self.collection.name)

        # Parsing job server list
        self.amqp = AMQPProducer(settings.conf.get('server', 'broker'))

        # Local event queue
        self.bLoop = False
        self.lastQueueCheckTime = 0

        # Statsd
        self.ackMessage = partial(msgqueue.ackMessage, flushInterval=1)

    def _run(self):
        self.bLoop = True
        self.amqp.start()

        while self.bLoop:
            try:
                print self.taskStatus
                # no worker, no process
                if len(self.taskStatus) == 0 or len(filter(lambda (t, w): w == 0, self.taskStatus.items())) > 0:
                    log.warn("Found not assigned task. - %s" % filter(lambda (t, w): w == 0, self.taskStatus.items()))
                    sleep(1)
                    continue

                """
                Process MongoDB Queue
                """
                if self.pause:
                    sleep(1)
                    continue

                if len(self.exchangeInfo) == 0:
                    sleep(1)
                    log.warn("No Message exchange info.")
                    continue

                sentCount = Counter()
                startFeed = time.time()

                fetcher = msgqueue.burstFetchMessages(self.collection, limit=2000)
                if fetcher.count() == 0:
                    sleep(1)
                    log.info("NO DATA.")
                    continue

                for msg in fetcher:
                    if not self.bLoop:
                        msgqueue.flushAcks(self.collection)
                        break

                    try:
                        msg['_id'] = str(msg['_id'])

                        if 'time' in msg and isinstance(msg['time'], datetime.datetime):
                            msg['time'] = str(msg['time'])

                        if 'log_time' in msg and isinstance(msg['log_time'], datetime.datetime):
                            msg['log_time'] = str(msg['log_time'])

                    except Exception, e:
                        self.ackMessage(self.collection, msg['_id'])
                        log.error(e)
                        continue

                    if msg.get('log_type', None) == 'debugreport':
                        self.ackMessage(self.collection, msg['_id'])
                        continue

                    eventId = LOG_TYPE_MAP.get(msg['log_type'], None)
                    if not eventId:
                        log.error("Undefined Log type - %s" % msg)
                        self.ackMessage(self.collection, msg['_id'])
                        continue

                    sentCount[eventId] += 1
                    if eventId not in self.exchangeInfo:
                        # Wait only ANS/AAT Log.

                        if eventId in ['evtPlayerLog', 'evtNetworkLog']:
                            log.warn("WARNING!!!! no worker wait %s event!!!!!" % eventId)
                            sleep(1)
                            continue
                        else:
                            # TODO: temporary, remember!!!!
                            self.ackMessage(self.collection, msg['_id'])
                            continue
                        pass

                    events = self.exchangeInfo[eventId]
                    try:
                        if self.amqp.publish(events['exchange'], events['routing'], msg, {"hashID": msg[events['hashKey']]} if events['hashKey'] else None) == True:
                            self.ackMessage(self.collection, msg['_id'])
                            sentCount['sent'] += 1
                        else:
                            print "!"
                            continue
                    except KeyError:
                        # delete message
                        self.ackMessage(self.collection, msg['_id'])
                    except Exception, e:
                        log.error("Publish error - %s" % e)

                if sentCount['sent'] == 0:
                    log.info("%s No Data? - sleep" % self.name)
                    sleep(1)
                else:
                    msgqueue.flushAcks(self.collection)
                    # queue wait time
                    if sentCount['sent'] < 100:
                        sleep(1)
                    log.info("MONGO %s PROCESSED [%0.2f] - %d/%d (%s)" % (self.name, time.time() - startFeed, sentCount.pop('sent', 0), sentCount.pop('wait', 0), ', '.join(map(lambda (k, v): "%s: %s" % (k, v), sentCount.iteritems()))))
            except pymongo.errors.AutoReconnect, e:
                log.info("MongoDB AutoReconnect")
                pass
            except KeyboardInterrupt:
                log.info("Catch keyboard interrupt in msgfeeder")
                self.bLoop = False
            except Exception, e:
                log.error(e)

        log.info("Exit %s Feeder Thread" % self.name)

    def stop(self):
        log.info("Stop %s Data Manager.." % self.name)
        self.bLoop = False
        self.amqp.stop()

    def updateWorkerStatus(self, info):
        self.taskStatus = info.copy()
        log.error(self.taskStatus)

    def updateSubscribeEvents(self, info):
        try:
            exchangeInfo = dict()
            for (event, hashKey) in info.items():
                exchangeInfo[event] = dict(exchange="job.general", routing=event, hashKey=hashKey)

            self.exchangeInfo = exchangeInfo
        except Exception, e:
            log.error(e)

    def command(self, cmd):
        if cmd is None or len(cmd) == 0:
            return

        if cmd == "pause":
            self.pause = True
            log.info("Pause MessageFeeder")
        elif cmd == "resume":
            self.pause = False
            log.info("Resume MessageFeeder")
        else:
            log.error("UNKNOWN COMMAND : %s" % cmd)
