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


from kombu.pools import connections
from kombu import Connection

try:
    import scubagear.logger as log
except ImportError:
    import logging as log


_process_connections = {}


def AMQPConnectionFactory(url):
    global _process_connections

    if url not in _process_connections:
        log.error("Create Connection - %s" % url)
        #conn = _process_connections[url] = connections[Connection(url)].acquire(block=True)
        _process_connections[url] = Connection(url)
        _process_connections[url].connect()
        print dir(_process_connections[url])

    return _process_connections[url]
