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


"""
Created on 2015. 03. 04.

@author: jaylee

This module is not included in Open DD.
You should ignore this module.
"""
__author__ = 'jaylee'

#from threading import Timer
import time
import gevent


# use gevent to prevent hang-up on exit (HUP signal, Keyboard interrupt, etc).


class TimeKeeper(object):
	# static class variable
	OW_TASK_SUBSCRIBE_EVENTS = []
	OW_TASK_PUBLISH_EVENTS = ['evtTimeInterval']
	OW_USE_HASHING = False
	OW_HASH_KEY = None
	OW_NUM_WORKER = 1

	def publishEvent(self, event, params):
		# THIS METHOD WILL BE OVERRIDE
		# DO NOT EDIT THIS METHOD
		pass

	def __init__(self):
		#self.timer = Timer(60, self.tmCallback)
		#self.timer.start()
		gevent.spawn(self.tmCallback)

	# def tmCallback(self):
	# 	self.timer = Timer(60, self.tmCallback)
	# 	tmp = int(time.time())
	# 	for i in range(20):
	# 		self.publishEvent('evtTimeInterval', {'tmp':tmp, 'num':i})
	#
	# 	self.timer.start()

	def tmCallback(self):
		while True:
			gevent.sleep(60)
			tmp = int(time.time())
			for i in range(20):
				self.publishEvent('evtTimeInterval', {'tmp':tmp, 'num':i})



