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


import scubagear.config as config
import scubagear.logger as log

conf = config.ConfigHandler()

try:
    environment = conf.get('main', 'environment')
except Exception, e:
    log.error("cannot find 'environment' config - use 'production' environment")
    environment = 'production'

if not log.isInitialized() and conf.has_section('log'):
    filename = conf.get('log', 'file') if conf.has_option('log', 'file') else "log/henem.log"
    if environment == 'production':
        level = conf.get('log', 'level') if conf.has_option('log', 'level') else log.INFO
    else:
        level = log.DEBUG

    name = conf.get('log', 'name') if conf.has_option('log', 'name') else "Henem"

    log.setupLogger(filename, level, name)
