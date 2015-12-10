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


import cherrypy
import scubagear.logger as log


class IControl(object):
    def __init__(self, server):
        self.henem = server
        pass

    @cherrypy.expose
    def feeder(self, action="status"):
        """
        start/stop henem message feedfer
        http://url/henem/control/feeder?action=<command>

        :param action: command (start, stop, status)
        :return: result message
        """
        try:
            cmd = str(action).lower()
            if cmd not in ['start', 'stop', 'status']:
                return "NOT OK"

            if cmd == "stop":
                for feeder in self.henem.feeders:
                    feeder.command("pause")
            elif cmd == "start":
                for feeder in self.henem.feeders:
                    feeder.command("resume")
            elif cmd == "status":
                msg = ""
                for feeder in self.henem.feeders:
                    for feeder in self.henem.feeders:
                        msg += "message feeding" if not feeder.pause else "feeding stopped\n"

                return msg

            return "message feeding %s" % cmd
        except Exception, e:
            log.error("RESTAPI - feeder : %s" % e)
            raise cherrypy.HTTPError(500)