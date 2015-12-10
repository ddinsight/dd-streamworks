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
AnsLog Insert
Created on 2015. 03. 04.

@author: jaylee

This module is not included in Open DD.
You should ignore this module.
"""

__author__ = 'jaylee'

import worker
from worker import log

class ProcessAnsLog(object):
	# static class variable
	OW_TASK_SUBSCRIBE_EVENTS = ['evtNetworkLog']
	OW_TASK_PUBLISH_EVENTS = []
	OW_USE_HASHING = False
	OW_HASH_KEY = None
	OW_NUM_WORKER = 8

	def __init__(self):
		self.rdbCur = None

	def __del__(self):
		if self.rdbCur:
			worker.dbmanager.freeCursor(self.rdbCur)

	def logInsert(self, params):
		try:
			if params.has_key('tTM') == False:
				try:
					vrdict = params.get('__validityReport', '')
					if vrdict.has_key('tTM'):
						params['tTM'] = float(vrdict['tTM'][0][0].replace(",", "."))
				except Exception, e:
					log.error("logInsert tTM convert [androidID:%s, pkgName:%s, sID:%s, tTM:%s, numTotalHits:%s] %s" % \
							  (params.get('deviceID', '-'), params.get('pkgName', '-'), params.get('sID', -9), params.get('tTM', -9), params.get('numTotalHits', -9), e))
					return

			self.rdbCur = None
			self.rdbCur = worker.dbmanager.allocDictCursor('myapwave')
			log.info("logInsert: before tran")

			self.rdbCur.execute("START TRANSACTION")

			strSQL = """INSERT IGNORE INTO appsslog (
				deviceID,	pkgName,	sID,		tTM,			objid,		agentAllowAns,		ansLogVer,		ansVer,			avgTP,		bssid,
				cellId, 	duration,	estTP,		evtKey,			evtValue,	evtSyncID,			startCode,		exitCode,		startTime,	eventTime,
				initWifi,	model,		netType,	numTotalHits,	NWcharac,	playAppPackageName,	playTime,		activeDownload,	activeTime, totalDownload,
				linkspd,	apfreq,		ver,		verCode,		myfiOn,		myfiAP,				parentAppName, 	insTs)
				VALUES (
				'%s',	'%s',	%d,		%.3f,	'%s',	%s,		'%s',	'%s',	%s,		'%s',
				'%s', 	%s,		%s,		'%s',	%s,		%s,		%s,		%s,		%s,		%s,
				%s, 	'%s',	'%s',	%s,		'%s',	'%s',	%s,		%s,		%s, 	%s,
				%s,		%s,		'%s',	%s,		%s,		%s,		'%s', 	%s)""" % \
				 (params['deviceID'], params['pkgName'], int(params['sID']), float(params['tTM']), params.get('objid', ''),
				  int(params.get('agentAllowAns', -1)), params.get('ansLogVer', ''), params.get('ansVer', ''),
				  params.get('avgTP', 'NULL'), params.get('bssid', ''),
				  params.get('cellId', ''), params.get('duration', 'NULL'), params.get('estTP', 'NULL'),
				  params.get('evtKey', ''), params.get('evtValue', 'NULL'),
				  params.get('evtSyncID', 'NULL'), params.get('startCode', 'NULL'), params.get('exitCode', 'NULL'),
				  params.get('startTime', 'NULL'), params.get('eventTime', 'NULL'),
				  params.get('initWifi', 'NULL'), params.get('model', ''), params.get('netType', ''),
				  params.get('numTotalHits', 'NULL'), params.get('NWcharac', ''),
				  params.get('playAppPackageName', ''), params.get('playTime', 'NULL'), params.get('activeDownload', 'NULL'),
				  params.get('activeTime', 'NULL'),params.get('totalDownload', 'NULL'),
				  params.get('lnkspd', 'NULL'), params.get('apfreq', 'NULL'), params.get('ver', ''),
				  params.get('verCode', 'NULL'), params.get('myfiOn', 'NULL'),
				  params.get('myfiAP', 'NULL'), params.get('parentAppName', ''), params.get('insTs', 'NULL')
				 )
			ret = self.rdbCur.execute(strSQL)

			strSQL = """INSERT IGNORE INTO appssdist (deviceID, isbusy, lstuptmp) VALUES ('%s', '0', unix_timestamp())""" %  (params['deviceID'])
			ret = self.rdbCur.execute(strSQL)

			self.rdbCur.execute("COMMIT")
			log.info("logInsert: commit tran")

		except Exception, e:
			self.rdbCur.execute("ROLLBACK")
			log.error("logInsert [androidID:%s, pkgName:%s, sID:%s, tTM:%s, numTotalHits:%s] %s" % \
					  (params.get('deviceID', '-'), params.get('pkgName', '-'), params.get('sID', -9), params.get('tTM', -9), params.get('numTotalHits', -9), e))
			raise e

		finally:
			if self.rdbCur <> None:
				worker.dbmanager.freeCursor(self.rdbCur)

	def publishEvent(self, event, params):
		# THIS METHOD WILL BE OVERRIDE
		# DO NOT EDIT THIS METHOD
		pass

	def handler(self, params):
		try:
			log.info("evtNetworkLog get")

			#cocoa Log skip
			if params.has_key('evtKey') == False and params.has_key('startTime') == False:
				return

			# 구 버전의 로그 무시
			apatLogVer = map(int, params.get('ver', "1.00.0").split('.'))
			if apatLogVer < [1,0,10]:
				return

			dataVer = map(int, params.get('ansLogVer', "0.5").split('.'))
			if dataVer < [1,0] and params.has_key('evtKey') == False:
				return

			self.logInsert(params)
			log.info("logInsert done")

		except Exception, e:
			log.error("processAnsLog ERROR[deviceID:%s, sID:%s, numTotalHits:%s] %s" % (params['deviceID'], params['sID'], params['numTotalHits'], e) )
			if str(e).find('restart') > 0:
				log.error("processAnsLog raise e")
				raise e

		return

