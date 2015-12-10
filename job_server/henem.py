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

import datetime
import signal
from functools import partial

import cherrypy
from cherrypy.process.plugins import PIDFile

import settings
from settings import conf as config
from settings import log as log
from server import HenemServer

from restapi.control import IControl
from gevent import monkey
monkey.patch_all()


def http_methods_allowed(methods=['GET', 'HEAD']):
    method = cherrypy.request.method.upper()
    if method not in methods:
        cherrypy.response.headers['Allow'] = ", ".join(methods)
        raise cherrypy.HTTPError(405)


def signalHandler(signum, frame, server):
    log.info("Recv Signal - %s" % signum)
    try:
        if server.running:
            log.error("!!!!!! STOP")
            server.stop()
        else:
            log.error("!!!!!! KILL")
            gevent.kill(server)
    except Exception, e:
        pass


class ManagementInterface(object):
    def __init__(self, server):
        self.control = IControl(server)


if __name__ == "__main__":
    server = HenemServer()

    if settings.environment == 'production':
        cherrypy.config.update({'environment': 'production', 'engine.autoreload_on': False})
    else:
        cherrypy.config.update({'engine.autoreload_on': False})

    PIDFile(cherrypy.engine, config.get('main', 'pid-file')).subscribe()

    signal.signal(signal.SIGTERM, partial(signalHandler, server=server))
    signal.signal(signal.SIGHUP, partial(signalHandler, server=server))
    signal.signal(signal.SIGINT, partial(signalHandler, server=server))

    log.info("Start Henem Server, environment - %s" % settings.environment)

    bindAddr = config.get('main', 'bind')
    bindPort = config.getint('main', 'port')

    log.info("bind server - %s:%s" % (bindAddr, bindPort))

    cherrypy.tools.allow = cherrypy.Tool('on_start_resource', http_methods_allowed)
    cherrypy.server.bind_addr = (bindAddr, bindPort)

    log.info("Start server...")
    server.start()

    try:
        cherrypy.tree.mount(ManagementInterface(server), '/henem')
        cherrypy.engine.start()
        server.join()
    except KeyboardInterrupt, e:
        log.info("KEYBOARD INTERRUPT!!!!!!")
    except Exception, e:
        log.error(e)

    log.info("Waiting for server stop.")
    cherrypy.engine.stop()

    if server.running:
        server.stop()
    server.join(3)

    log.info("Stop server - %s" % datetime.datetime.now())

