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


import sys
from settings import *
import json
import shutil
import pymongo
import gridfs
import tarfile
import importlib
import lockfile

try:
    import scubagear.logger as log
except ImportError:
    import logging as log


class TaskModule(object):
    def __init__(self, url):
        self.url = url
        self.client = None
        self.db = None
        self.fs = None
        self.modules = dict()
        self.lock = lockfile.LockFile(os.path.join(DIR_TASK_MODULE, '__lock'))

    def open(self):
        if not self.client:
            self.client = pymongo.MongoClient(self.url)
            db = self.client.get_default_database()
            self.fs = gridfs.GridFS(db)

    def close(self):
        self.client.close()
        self.fs = None
        self.client = None

    def cleanup(self):
        # Remove module dir not in 'info.json'
        localModule = self.lockAndGetLocalModuleInfo()

        if os.path.exists(DIR_TASK_MODULE):
            for name in os.listdir(DIR_TASK_MODULE):
                fullpath = os.path.join(DIR_TASK_MODULE, name)
                if os.path.isdir(fullpath) and not os.path.islink(fullpath):
                    if name not in localModule:
                        shutil.rmtree(fullpath)

        self.unlockAndUpdateModuleInfo()

    def lockAndGetLocalModuleInfo(self):
        try:
            self.lock.acquire(timeout=5)
        except lockfile.LockTimeout, e:
            raise e
        except Exception, e:
            raise e

        localModule = {}

        try:
            fp = open(os.path.join(DIR_TASK_MODULE, 'info.json'), 'r')
            localModule = json.load(fp)
            fp.close()
        except IOError:
            pass
        except TypeError:
            localModule = {}
        except Exception, e:
            print e
            self.lock.release()
            raise e

        return localModule

    def unlockAndUpdateModuleInfo(self, info=None):
        if info is not None:
            try:
                fp = open(os.path.join(DIR_TASK_MODULE, 'info.json'), 'w')
                json.dump(info, fp)
                fp.close()
            except Exception, e:
                self.lock.release()
                raise e

        self.lock.release()

    def remove(self, module):
        try:
            localModule = self.lockAndGetLocalModuleInfo()
        except Exception, e:
            raise e

        # unimport module
        try:
            del sys.modules[module]
        except KeyError, e:
            #print "Unload Error - %s" % module
            pass

        try:
            self.modules.pop(module)
        except KeyError:
            #print "Key Error - self.modules[%s]" % module
            pass

        if module in localModule:
            modulePath = os.path.join(DIR_TASK_MODULE, module)

            if os.path.exists(modulePath):
                if os.path.isdir(modulePath) and not os.path.islink(modulePath):
                    shutil.rmtree(modulePath)
                else:
                    os.remove(modulePath)

            try:
                localModule.pop(module)
            except KeyError:
                #print "Key Error - localModule[%s]" % module
                pass

        self.unlockAndUpdateModuleInfo(localModule)

        return True

    def get(self, module):
        localModule = self.lockAndGetLocalModuleInfo()

        try:
            for mod in self.fs.find({'module': module}).sort('uploadDate', -1).limit(1):
                # need download?
                if (module not in localModule) or (str(mod._id) != localModule[module]['id']):
                    log.info("Downloading %s" % module)

                    # Download
                    fd = open('%s/%s' % (DIR_TASK_MODULE, mod.filename), 'wb')
                    fd.write(mod.read())
                    fd.close()
                    mod.close()

                    modulePath = os.path.join(DIR_TASK_MODULE, module)
                    if os.path.exists(modulePath) and os.path.exists('%s/__init__.py' % modulePath):
                        shutil.rmtree(modulePath)

                    # Extract to module path
                    tar = tarfile.open('%s/%s' % (DIR_TASK_MODULE, mod.filename), 'r:bz2')
                    tar.extractall(DIR_TASK_MODULE)
                    tar.close()

                    # remove download file
                    os.remove('%s/%s' % (DIR_TASK_MODULE, mod.filename))
                    localModule[module] = {'id': str(mod._id)}
                    log.info("Download complete %s" % module)

                if (module not in self.modules) or (localModule[module]['id'] != self.modules[module]['id']):
                    if module in sys.modules:
                        #print "Re-loading %s - %s" % (module, os.getpid())
                        modObj = sys.modules[module]
                        reload(modObj)
                    else:
                        #print "Loading %s - %s" % (module, os.getpid())
                        modObj = importlib.import_module(module, DIR_TASK_MODULE)

                    self.modules[module] = localModule[module]
        except Exception, e:
            self.unlockAndUpdateModuleInfo()
            raise e

        self.unlockAndUpdateModuleInfo(localModule)

    def getModuleList(self):
        return self.modules.keys()