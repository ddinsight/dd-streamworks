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


import os
import scubagear.config as config
import uuid

MAX_SLOT_NUMBER = 8
DIR_TASK_MODULE = 'taskmodule'
DIR_DEV_MODULE = 'devmodule/development'

conf = config.ConfigHandler()
UUID = uuid.uuid4()

if 'OW_ZOOKEEPER' in os.environ:
    ZK_URL = os.environ['OW_ZOOKEEPER']
    # override conf
    conf.set('server', 'meta', ZK_URL)
elif conf.has_section('server') and conf.has_option('server', 'meta'):
    ZK_URL = conf.get('server', 'meta')
else:
    ZK_URL = 'zk://127.0.0.1:2181/ddinsight'

print ZK_URL