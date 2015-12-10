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

This module is not included in Open DD.
You should ignore this module.
"""

__author__ = 'jaylee'

import time
import worker
from worker import log
import os
import random

appss = None
lastParamTmp = 0

class AppSession(object):
	def __init__(self):
		self.rdbCur = None

	def __del__(self):
		if self.rdbCur:
			worker.dbmanager.freeCursor(self.rdbCur)

	def __getGeoLoc(self, cellId = "", bssid = ""):

		cellValid = False
		if cellId <> None and cellId > "":
			celllst = cellId.split('_')
			if len(celllst) == 3 and celllst[0].isdigit() and celllst[1].isdigit() and celllst[2].isdigit():
				cellValid = True

		bssidValid = False
		if bssid <> None and bssid > "":
			bidlst = bssid.split(':')
			if len(bidlst) == 6:
				bssidValid = True

		loc = None
		if cellValid == True:
			ret = self.rdbCur.execute("SELECT lng, lat FROM apmain.cellinfo WHERE fullid='%s' and (lat > -9999 or lng > -9999)" % cellId)
			if ret:
				row = self.rdbCur.fetchone()
				loc = (row['lng'], row['lat'], 0, {})
			elif bssidValid == True:
				loc = getGeoLocFromAP(bssid)
		elif bssidValid == True:
			loc = getGeoLocFromAP(bssid)

		if loc == None:
			loc = (-9999, -9999, 0, {})

		return loc

	def insertNetNode(self, session, data):
		try:
			# if anslog is just event log, there is no data.
			if data.get('startTime', 0) == 0 or data.get('startTime', 0) >= data.get('eventTime', 0):
				return

			sqlStr = ""

			# get anslog's data
			cellid = data.get('cellId', '')
			if cellid == None :
				cellid = ''

			snetType  = '' if data.get('netType', None) == None else data['netType']

			if snetType == '0' or snetType.upper().find('WIFI')>=0:
				ntype = 'WIFI'
				netid = '' if data.get('bssid', None) == None else data['bssid']
			else:
				ntype = snetType[0:10]
				netid = cellid

			ustm = data['startTime']
			uetm = data['eventTime']
			uactbytes = 0 if data.get('activeDownload', None) == None else data['activeDownload']
			uactdur = 0 if data.get('activeTime', None) == None else data['activeTime']
			uexitcode = -99 if data.get('exitCode', None) == None else data['exitCode']
			umyfion = -1 if data.get('myfiOn', None) == None else data['myfiOn']
			umyfiap = -1 if data.get('myfiAP', None) == None else data['myfiAP']


			if session.get('bsessionID', '') == '':
				sqlStr = """INSERT INTO appsession_net
	(sessionID, ntype, stm, etm, netid, cellId, actdur, actbytes, exitcode, myfion, myfiap, lstuptmp)
	VALUES ('%s', '%s', %d, %d, '%s', '%s', %d, %d, %d, %d, %d, unix_timestamp())
    ON DUPLICATE KEY UPDATE
    etm = VALUES(etm), netid = VALUES(netid), actdur = VALUES(actdur), actbytes = VALUES(actbytes),
    cellId = VALUES(cellId), exitcode = VALUES(exitcode), myfion = VALUES(myfion), myfiap = VALUES(myfiap) """ % \
				         (session['sessionID'], ntype, ustm, uetm, netid, cellid,
				          uactdur, uactbytes, uexitcode, umyfion, umyfiap)
				self.rdbCur.execute(sqlStr)
			else:
				if session['nt'] == ntype and session['bnetid'] == netid:
					strList = []
					strList.append("etm = %d" % uetm)
					if session['bcellid'] == '':
						strList.append("cellId = '%s'" % cellid)
					strList.append("actdur = %d" % (session['bactdur'] + uactdur))
					strList.append("actbytes = %d" % (session['bactbytes'] + uactbytes))
					strList.append("exitCode = %d" % uexitcode)
					if session['bmyfion'] < 0 and umyfion >= 0:
						strList.append("myfion = %d" % umyfion)
					if session['bmyfiap'] < 0 and umyfiap >= 0:
						strList.append("myfiap = %d" % umyfiap)

					sqlStr = """UPDATE appsession_net
	SET %s, lstuptmp = unix_timestamp()
    WHERE sessionID = '%s' and ntype = '%s' and stm = %d""" % \
					         (", ".join(strList), session['bsessionID'], session['nt'], session['bstm'])
					self.rdbCur.execute(sqlStr)

					if session['csessionID'] > '':
						if session['ccellid'] == cellid:
							sqlStr = """UPDATE appsession_netcell
	SET etm = %d, exitCode = %d, lstuptmp = unix_timestamp()
    WHERE sessionID = '%s' and ntype = '%s' and stm = %d and ccstm = %d""" % \
							         (uetm, uexitcode, session['csessionID'], session['nt'], session['bstm'], session['ccstm'])
							self.rdbCur.execute(sqlStr)
						else:
							sqlStr = """INSERT appsession_netcell
	(sessionID, ntype, stm, ccstm, etm, cellId, exitCode, lstuptmp)
	VALUES ('%s', '%s', %d, %d, %d, '%s', %d, unix_timestamp())
    ON DUPLICATE KEY UPDATE
    etm = VALUES(etm), cellId = VALUES(cellId), exitCode = VALUES(exitCode) """ % \
							         (session['csessionID'], session['nt'], session['bstm'], ustm, uetm, cellid, uexitcode)
							self.rdbCur.execute(sqlStr)

					elif session['bcellid'] <> cellid:
						#insert 2 records into netcell table.
						sqlStr = """INSERT appsession_netcell
	(sessionID, ntype, stm, ccstm, etm, cellId, exitCode, lstuptmp)
	VALUES ('%s', '%s', %d, %d, %d, '%s', %d, unix_timestamp()), ('%s', '%s', %d, %d, %d, '%s', %d, unix_timestamp())
    ON DUPLICATE KEY UPDATE
    etm = VALUES(etm), cellId = VALUES(cellId), exitCode = VALUES(exitCode) """ % \
						         (session['bsessionID'], session['nt'], session['bstm'], session['bstm'],
						          session['betm'], session['bcellid'], session['bexitCode'],
						          session['bsessionID'], session['nt'], session['bstm'], ustm, uetm, cellid, uexitcode)
						self.rdbCur.execute(sqlStr)

				else:
					sqlStr = """INSERT INTO appsession_net
	(sessionID, ntype, stm, etm, netid, cellId, actdur, actbytes, exitcode, myfion, myfiap, lstuptmp)
	VALUES ('%s', '%s', %d, %d, '%s', '%s', %d, %d, %d, %d, %d, unix_timestamp())
    ON DUPLICATE KEY UPDATE
    etm = VALUES(etm), netid = VALUES(netid), actdur = VALUES(actdur), actbytes = VALUES(actbytes),
    cellId = VALUES(cellId), exitcode = VALUES(exitcode), myfion = VALUES(myfion), myfiap = VALUES(myfiap) """ % \
					         (session['sessionID'], ntype, ustm, uetm, netid, cellid,
					          uactdur, uactbytes, uexitcode, umyfion, umyfiap)
					self.rdbCur.execute(sqlStr)


		except Exception, e:
			log.error("insertNetNode : %s [SQL:%s]" % (e, sqlStr))
			log.error(data)
			raise e

	def updateSession(self, session, data):
		try:
			updateParams = {}

			#===================================================================
			# if data.has_key('evtKey'):
			# 	startCode = int(data['evtValue'])
			# 	updateParams['exitCode'] = -99
			# else:
			# 	startCode = int(data['startCode'])
			# 	updateParams['exitCode'] = int(data.get('exitCode', -99))
			#===================================================================

			startCode = int(data.get('startCode', -99))
			evtCode = int(data.get('evtValue', -99))
			exitCode = int(data.get('exitCode', -99))

			evtStartCode = startCode if startCode > 0 else evtCode
			evtExitCode = exitCode if exitCode > 0 else evtCode

			updateParams['exitCode'] = evtExitCode

			if evtStartCode == 70 and session['statAppss'] <> '2':
				updateParams['statAppss'] = '2'

			if evtExitCode in (80, 82, 91, 92, 99) or startCode in (80, 82) \
					or ((session['statAppss'] <> '2' and evtStartCode <> 70) and (evtExitCode in (20, 40, 60) or startCode in (20, 40, 60))):
				if session['statAppss'] <> '0':
					updateParams['statAppss'] = '0'

			if evtExitCode in (40, 60):
				updateParams['mode'] = 1
			elif evtStartCode in (30, 50):
				updateParams['mode'] = 0

			if data.get('evtKey', '') == '':
				updateParams['endTime'] = int(data.get('eventTime', 0))

				if int(session['startTime']) == 0:
					updateParams['startTime'] = int(data.get('startTime', 0))
					updateParams['startCode'] = startCode

				if int(session['agentMode']) == -1:
					updateParams['agentMode'] = int(data.get('agentAllowAns', -1))

				if int(session['agentMode']) > 0 and int(session['agentMode']) != int(data['agentAllowAns']):
					updateParams['agentModeChanged'] = 1

				if session['ansVersion'] == '':
					updateParams['ansVersion'] = data.get('ansVer', '')

				if session['initialWifi'] == -1:
					updateParams['initialWifi'] = data.get('initWifi', -1)

				if session['parentappnm'] == '' and data.get('parentAppName', '') > '':
					updateParams['parentappnm'] = data.get('parentAppName', '')

				if int(data.get('initWifi', -1)) == 1 and int(data.get('startCode', -1)) == 30:   #ANS agent는 현재 foreground올라올 때만 initwifi check.
					updateParams['initialWifiOnCount'] = session['initialWifiOnCount'] + 1
				elif int(data.get('initWifi', -1)) == 0 and int(data.get('startCode', -1)) == 30:
					updateParams['initialWifiOffCount'] = session['initialWifiOffCount'] + 1


				if data.get('cellId', None) <> None and data.get('cellId', '') > '':
					cellId = data.get('cellId')
					cellIdLst = cellId.split('_')
					if len(cellIdLst) == 3 and cellIdLst[0].isdigit():
						if session['countryCode'] == 0:
							plmnId = cellIdLst[0]
							if int(plmnId) > 0:
								updateParams['countryCode'] = plmnId[0:3]
								updateParams['operatorCode'] = plmnId[3:6]

				loc = self.__getGeoLoc(data.get('cellId', ''), data.get('bssid', ''))

				#               if session['countryCode'] == 0 and data.has_key('cellId'):
				if session['beginLong'] < -200 and loc[0] > -200:
					updateParams['beginLong'] = loc[0]
					updateParams['beginLat'] = loc[1]

				#               if int(session['lastLong'] * 100) != int(loc[0] * 100) or int(session['lastLat'] * 100) != int(loc[1] * 100):
				if loc[0] > -200:
					updateParams['lastLong'] = loc[0]
					updateParams['lastLat'] = loc[1]

				#                   geoList = json.loads(session['geoLocationList'])
				#                   geoList.append([updateParams['lastLong'], updateParams['lastLat']])
				#                   updateParams['geoLocationList'] = json.dumps(geoList)

				if data.get('netType', '') == 0 or data.get('netType', '').upper().find('WIFI')>=0:
					type = 'wifi'
					networkId = data.get('bssid', '-')
				else:
					type = data.get('netType', '')
					networkId = data.get('cellId', '-')

				traffic = int(data.get('totalDownload', 0))
				activeTraffic = int(data.get('activeDownload', 0))

				duration = int(data.get('duration', 0))
				playDuration = int(data.get('playTime', 0))

				checkList = [traffic, activeTraffic]
				[traffic, activeTraffic] = map(lambda x: 0 if x < 0 else x, checkList)

				checkList = [duration, playDuration]
				[duration, playDuration] = map(lambda x: 0 if (x < 0) or (x > 1800) else x, checkList)

				traffic = self.clippingTP(traffic, duration)
				activeTraffic = self.clippingTP(activeTraffic, duration)

				if type == 'wifi':
					updateParams['wifiTraffic'] = session['wifiTraffic'] + traffic
					updateParams['wifiActiveTraffic'] = session['wifiActiveTraffic'] + activeTraffic
					updateParams['wifiPlayTime'] = session['wifiPlayTime'] + playDuration
					updateParams['wifiConnectedDuration'] = session['wifiConnectedDuration'] + duration
				else:
					updateParams['mobileTraffic'] = session['mobileTraffic'] + traffic
					updateParams['mobileActiveTraffic'] = session['mobileActiveTraffic'] + activeTraffic
					updateParams['mobilePlayTime'] = session['mobilePlayTime'] + playDuration
					updateParams['mobileConnectedDuration'] = session['mobileConnectedDuration'] + duration

				#try:
				#   networkList = json.loads(session['networkList'])
				#except Exception, e:
				#   networkList = []
				if session['bnetid'] <> networkId and data['startTime'] < data['eventTime']:
					if session['nt'].find('WIFI') >= 0 and type <> 'wifi':
						updateParams['wifiToMobileChangeCount'] = session['wifiToMobileChangeCount'] + 1
					elif session['nt'].find('WIFI') < 0 and session['nt'] > '' and  type == 'wifi':
						updateParams['mobileToWifiChangeCount'] = session['mobileToWifiChangeCount'] + 1

				if type == 'wifi':
					if data.get('agentAllowAns') == False or \
							(updateParams.get('wifiToMobileChangeCount', session['wifiToMobileChangeCount']) == 0 and \
										 updateParams.get('mobileToWifiChangeCount', session['mobileToWifiChangeCount']) == 0):
						updateParams['wifiTrafficBySys'] = session['wifiTrafficBySys'] + traffic
					else:
						updateParams['wifiTrafficByMAO'] = session['wifiTrafficByMAO'] + traffic

			if len(updateParams) > 0:
				sqlStr = "UPDATE appsession SET %s, lstuptmp = unix_timestamp() WHERE sessionID ='%s'" % (','.join(map(lambda key:"%s='%s'" % (key, updateParams[key]), updateParams)), session['sessionID'])
				self.rdbCur.execute(sqlStr)

				self.insertNetNode(session, data)

		except Exception, e:
			log.error("appSession updateSession Error[androidID:%s, numTotalHits:%s] %s" % (data['deviceID'], data['numTotalHits'], e))
			log.error(session)
			raise e

	def makeSession(self, data):
		try:
			session = {}

			session['valid'] = 1

			session['androidID'] = data.get('deviceID')
			session['pkgnm'] = data.get('pkgName')
			session['productModel'] = data.get('model')  #

			session['startTime'] = int(data.get('startTime', 0))
			if data.get('evtKey', '') > '':
				session['endTime'] = int(data.get('tTM', 0))
			else:
				session['endTime'] = int(data.get('eventTime', 0))
			session['versionCode'] = data.get('verCode', 0)
			session['sID'] = int(data.get('sID', 0))
			session['sessionID'] = "%s_%s" % (data['deviceID'], ("%.3f" % data['tTM']).replace('.',''))

			startCode = int(data.get('startCode', -99))
			evtCode = int(data.get('evtValue', -99))
			exitCode = int(data.get('exitCode', -99))
			evtStartCode = startCode if startCode > 0 else evtCode
			evtExitCode = exitCode if exitCode > 0 else evtCode

			session['startCode'] = evtStartCode
			session['exitCode'] = evtExitCode

			if evtStartCode == 70:
				session['statAppss'] = '2'
			if evtExitCode in (80, 82, 91, 92, 99) or startCode in (80, 82) \
					or (evtStartCode <> 70 and (evtExitCode in (20, 40, 60) or startCode in (20, 40, 60))):
				#if session end code comes when session create, set stat = 0
				session['statAppss'] = '0'
			elif evtStartCode <> 70:
				session['statAppss'] = '1'

			if evtExitCode in (40, 60):
				session['mode'] = 1
			elif evtStartCode in (30, 50):
				session['mode'] = 0
			else:
				session['mode'] = 0

			#player Package
			if data.get('playAppPackageName', '') > '':
				pkgStr = data['playAppPackageName'].split(';')[0]   #in case multi player app support
				pkgStrList = pkgStr.split('/')
				if len(pkgStrList) >= 2:
					session['apppkgnm'] = pkgStr.split('/')[0]
					session['appvercd'] = pkgStr.split('/')[1]

			if data.get('evtKey', '') > '':
				session['agentMode'] = -1
				session['ansVersion'] = ''
				session['initialWifi'] = -1
				session['initialWifiOnCount'] = 0
				session['initialWifiOffCount'] = 0
				session['wifiTraffic'] = 0
				session['wifiActiveTraffic'] = 0
				session['wifiConnectedDuration'] = 0
				session['wifiPlayTime'] = 0
				session['wifiTrafficBySys'] = 0
				session['wifiTrafficByMAO'] = 0
				session['mobileTraffic'] = 0
				session['mobileActiveTraffic'] = 0
				session['mobileConnectedDuration'] = 0
				session['mobilePlayTime'] = 0
				#networkList = []
				#session['networkList'] = json.dumps(networkList)
				session['countryCode'] = 0
				session['operatorCode'] = 0

				session['beginLong'] = -9999
				session['beginLat'] = -9999
				session['lastLong'] = session['beginLong']
				session['lastLat'] = session['beginLat']

				session['mobileToWifiChangeCount'] = 0
				session['wifiToMobileChangeCount'] = 0
				session['parentappnm'] = ''
				#remove in new table.
				#geoList = []
				#session['geoLocationList'] = json.dumps(geoList)
			else:
				session['agentMode'] = int(data.get('agentAllowAns', -1))
				session['ansVersion'] = data.get('ansVer', '')  #j

				if data.get('netType', '') == 0 or data.get('netType', '').upper().find('WIFI')>=0:
					type = 'wifi'
				else:
					type = data.get('netType', '')

				session['initialWifi'] = data.get('initWifi', -1)

				if session['initialWifi'] == 1 and session['startCode'] == 30 :
					session['initialWifiOnCount'] = 1
					session['initialWifiOffCount'] = 0
				elif session['initialWifi'] == 0 and session['startCode'] == 30 :
					session['initialWifiOnCount'] = 0
					session['initialWifiOffCount'] = 1
				else:
					session['initialWifiOnCount'] = 0
					session['initialWifiOffCount'] = 0

				traffic = int(data.get('totalDownload', 0))
				activeTraffic = int(data.get('activeDownload', 0))

				duration = int(data.get('duration', 0))
				playDuration = int(data.get('playTime', 0))

				checkList = [traffic, activeTraffic]
				[traffic, activeTraffic] = map(lambda x: 0 if x < 0 else x, checkList)

				checkList = [duration, playDuration]
				[duration, playDuration] = map(lambda x: 0 if x < 0 or x > 1800 else x, checkList)

				traffic = self.clippingTP(traffic, duration)
				activeTraffic = self.clippingTP(activeTraffic, duration)

				if type == 'wifi':
					session['wifiTraffic'] = traffic
					session['wifiActiveTraffic'] = activeTraffic
					session['wifiConnectedDuration'] = duration
					session['wifiPlayTime'] = playDuration
					session['wifiTrafficBySys'] = traffic
					session['wifiTrafficByMAO'] = 0
					#mobile variables initialize
					session['mobileTraffic'] = 0
					session['mobileActiveTraffic'] = 0
					session['mobileConnectedDuration'] = 0
					session['mobilePlayTime'] = 0
				else:
					session['mobileTraffic'] = traffic
					session['mobileActiveTraffic'] = activeTraffic
					session['mobileConnectedDuration'] = duration
					session['mobilePlayTime'] = playDuration
					#wifi variables initialize
					session['wifiTraffic'] = 0
					session['wifiActiveTraffic'] = 0
					session['wifiConnectedDuration'] = 0
					session['wifiPlayTime'] = 0
					session['wifiTrafficBySys'] = 0
					session['wifiTrafficByMAO'] = 0

				#networkList = []
				#networkList.append([type, networkId, traffic, data.get('eventTime'), data.get('eventTime')])  #
				#session['networkList'] = json.dumps(networkList)

				# 위치정보
				session['countryCode'] = 0
				session['operatorCode'] = 0

				if data.get('cellId', None) <> None and data.get('cellId', '') > '':
					cellId = data.get('cellId')
					cellIdLst = cellId.split('_')
					if len(cellIdLst) == 3 and cellIdLst[0].isdigit():
						plmnId = cellIdLst[0]
						if int(plmnId) > 0:
							session['countryCode'] = plmnId[0:3]
							session['operatorCode'] = plmnId[3:6]

				loc = self.__getGeoLoc(data.get('cellId', ''), data.get('bssid', ''))	 #

				session['beginLong'] = loc[0]
				session['beginLat'] = loc[1]
				session['lastLong'] = session['beginLong']
				session['lastLat'] = session['beginLat']
				session['mobileToWifiChangeCount'] = 0
				session['wifiToMobileChangeCount'] = 0
				session['parentappnm'] = data.get('parentAppName', '')

				#geoList = []
				#geoList.append([loc[0], loc[1]])
				#session['geoLocationList'] = json.dumps(geoList)


			sqlStr = "INSERT INTO appsession (%s, lstuptmp) VALUES (%s, unix_timestamp())" % (','.join(session.keys()), str(map(str, session.values())).strip('[*]'))
			self.rdbCur.execute(sqlStr)

			self.updateAppssLst(data, session['sessionID'])

			self.insertNetNode(session, data)

			sqlStr = """INSERT IGNORE INTO GRP_USER (grpid, androidID, regdate, enddate, wrk_id)
    VALUES ('001', '%s', date_format(utc_timestamp(), '%%Y%%m%%d'), '99991231', 'wrk_log2wave')""" % (session['androidID'])
			self.rdbCur.execute(sqlStr)

		except Exception, e:
			log.error("appSession makeSession Error[androidID:%s, numTotalHits:%s] %s" % (data['deviceID'], data['numTotalHits'], e))
			log.error(session)
			raise e

	def closeSession(self, session, data):
		try:
			if session['startTime'] == session['endTime'] or session['startTime'] == 0:
				sqlStr = """DELETE vd, vbb, vbc
                FROM appsession AS vd
                    LEFT JOIN appsession_net AS vbb ON vd.sessionID = vbb.sessionID
                    LEFT JOIN appsession_netcell AS vbc ON vd.sessionID = vbc.sessionID
                WHERE vd.sessionID = '%s'""" % (session['sessionID'])
				self.rdbCur.execute(sqlStr)
				actionkey = 'D'
			else:
				sqlStr = "UPDATE appsession SET valid=1, statAppss = '0', lstuptmp = unix_timestamp() WHERE sessionID='%s'" % (session['sessionID'])
				self.rdbCur.execute(sqlStr)
				actionkey = 'U'

			anID = session['androidID']
			pkgname = session['pkgnm']
			appSSid = session['sessionID']
			sID = session['sID']

			sqlStr = """UPDATE vidsession
            SET appSessionIDSt = IF(appSessionIDSt = '%s', '', appSessionIDSt),
				appSessionIDEnd = IF(appSessionIDEnd = '%s', '', appSessionIDEnd),
				lstuptmp = unix_timestamp()
            WHERE (appSessionIDSt = '%s' or appSessionIDEnd = '%s') AND playSessionID like '%s%%' and androidID = '%s' and pkgnm = '%s' and sID = %d
            """ % (appSSid, appSSid, appSSid, appSSid, anID, anID, pkgname, sID)

			ret = self.rdbCur.execute(sqlStr)

			sqlStr = """UPDATE vidsession_log SET appSessionID = '' WHERE appSessionID = '%s' and playSessionID like '%s%%'""" % (appSSid, anID)
			ret = self.rdbCur.execute(sqlStr)

			if actionkey == 'U':
				sqlStr = """UPDATE vidsession
				SET appSessionIDSt = '%s', lstuptmp = unix_timestamp()
                WHERE playSessionID LIKE '%s%%' AND androidID = '%s' AND pkgnm = '%s' AND sID = %d
                      AND vidnetStartTime >= %d AND vidnetStartTime < %d""" % (appSSid, anID, anID, pkgname, sID, session['startTime'], session['endTime'])
				ret = self.rdbCur.execute(sqlStr)

				sqlStr = """UPDATE vidsession
				SET appSessionIDEnd = '%s', lstuptmp = unix_timestamp()
                WHERE playSessionID LIKE '%s%%' AND androidID = '%s' AND pkgnm = '%s' AND sID = %d
                      AND vidnetEndTime >= %d AND vidnetEndTime < %d""" % (appSSid, anID, anID, pkgname, sID, session['startTime'], session['endTime'])
				ret = self.rdbCur.execute(sqlStr)

				sqlStr = """UPDATE vidsession_log SET appSessionID = '%s', lstuptmp = unix_timestamp()
                WHERE playSessionID like '%s%%' and logStartTime >= %d and logStartTime < %d """ % (appSSid, anID, session['startTime'], session['endTime'])
				ret = self.rdbCur.execute(sqlStr)

		except Exception, e:
			log.error("appSession closeSession Error[sessionID:%s] %s" % (session['sessionID'], e))
			raise e

	def _closePrevAndMakeNewSession(self, session, data):
		self.closeSession(session, data)
		try:
			self.makeSession(data)
		except Exception, e:
			log.error("makeSession Error:%s" % e)
			raise e
		#===============================================================================
		# # process evtvalue as evtvalue.
		#
		# 	def preprocessEvent(self, data):
		# 		#actCode와 reason code 매핑
		# # 		codeMap = {10:21, 20:12, 30:20, 40:19, 50:22, 60:14}
		# 		if not data.has_key('evtKey') or not data.has_key('evtValue'):
		# 			return None
		#
		# 		if data['evtKey'] != 'actCode':
		# 			return None
		#
		# 		data['exitCode'] = int(data['evtValue'])
		# 		return data
		#===============================================================================

	def preprocessLog(self, data):
		#exit code conversion
		codeMap = {15:0, 21:10, 12:20, 20:30, 19:40, 22:50, 14:60, 13:100, 16:120, 23:121,
		           7:141, 6:201, 5:0, 17:202, 0:301, 3:302, 2:0, 1:303, 4:304, 11:0, 10:0,
		           8:305, 9:306, 24:307, -1:402}
		try:
			dataVer = map(int, data['ansLogVer'].split('.'))
			if dataVer < [2,0]:
				data['exitCode'] = codeMap.get(int(data.get('exitCode', -99)), -99)
				data['startCode'] = codeMap.get(int(data.get('startCode', -99)), -99)
			else:
				data['exitCode'] = int(data.get('exitCode', -99))
				data['startCode'] = int(data.get('startCode', -99))
		except Exception, e:
			log.error("exitCode field Error %s:%s" % (e, data.get('exitCode')))
			data['exitCode'] = 0
		# correct negative value
		try:
			data['totalDownload'] = int(data.get('totalDownload', 0)) if int(data.get('totalDownload', 0)) >=0 else 0
		except Exception, e:
			log.error("totalDownload field Error %s:%s" % (e, data.get('totalDownload')))
			data['totalDownload'] = 0

		try:
			data['activeDownload'] = int(data.get('activeDownload', 0)) if int(data.get('activeDownload', 0)) >= 0 else 0
		except Exception, e:
			log.error("activeDownload field Error %s:%s" % (e, data.get('activeDownload')))
			data['activeDownload'] = 0

		try:
			data['duration'] = int(data.get('duration', 0)) if int(data.get('duration', 0)) >= 0 else 0
		except Exception, e:
			log.error("duration field Error %s:%s" % (e, data.get('duration')))
			data['duration'] = 0

		try:
			data['playTime'] = int(data.get('playTime', 0)) if int(data.get('playTime', 0)) >= 0 else 0
		except Exception, e:
			log.error("playTime field Error %s:%s" % (e, data.get('playTime')))
			data['playTime'] = 0

		data['initWifi'] = int(data.get('initWifi', -1))
		data['agentAllowAns'] = int(data.get('agentAllowAns', -1))

		cellId = data.get("cellId", "0_0_0").split('_')
		if len(cellId) != 3:
			data.pop('cellId')

		return data

	def updateAppssLst(self, data, sessionID):
		try:
			strSQL = """REPLACE appsession_lst
    (androidID, pkgnm, tTM, numTotalHits, sID,sessionID, oid)
    VALUES ('%s', '%s', %.3f, %d, %d, '%s', '%s') """ % (data['deviceID'], data['pkgName'],
			                                             float(data['tTM']), int(data['numTotalHits']), int(data.get('sID', 0)), sessionID, data.get('objID', ''))

			ret = self.rdbCur.execute(strSQL)
			if ret == 0:
				log.warn("UPDATE appsession_lst 0 row strSQL:%s" % strSQL)

		except Exception, e:
			log.error("update appsession_lst :%s" % e)
			log.error(strSQL)
			raise e

	def procSyncID(self, data, sessionID):
		try:
			self.rdbCur.execute("START TRANSACTION")
			tbCols = ['deviceID', 'pkgName', 'sID', 'evtSyncID', 'tTM', 'numTotalHits', 'evtValue', 'eventTime', 'startTime', 'startCode', 'exitCode',
			          'activeDownload', 'activeTime', 'agentAllowAns', 'ansLogVer', 'ansVer', 'avgTP', 'bssid', 'cellId', 'duration', 'initWifi', 'model',
			          'netType', 'NWcharac', 'playAppPackageName', 'parentAppName', 'playTime', 'totalDownload', 'ver', 'verCode', 'myfiOn', 'myfiAP']

			strSQL = """SELECT * FROM appsync WHERE deviceID = '%s' and pkgName = '%s' and sID = %d and evtSyncID = %d
                    """ % (data.get('deviceID', ''), data.get('pkgName', ''), int(data.get('sID', 0)), int(data.get('evtSyncID', 0)))
			retFunc = self.rdbCur.execute(strSQL)
			if retFunc == 0:
				sqlDict = {}
				for col in tbCols:
					if data.has_key(col):
						if col == 'initWifi' or col == 'agentAllowAns':
							if str(data[col]) in ['true', 'True', '1']:
								sqlDict[col] = 1
							elif str(data[col]) in ['false', 'False', '0']:
								sqlDict[col] = 0
							else:
								sqlDict[col] = -1
						else:
							sqlDict[col] = data[col]
				#sqlStr = "INSERT INTO appsession (%s) VALUES (%s)" % (','.join(session.keys()), str(map(str, session.values())).strip('[*]'))
				strSQL = "INSERT INTO appsync (%s, lstuptmp) VALUES (%s, unix_timestamp())" % (','.join(sqlDict.keys()), str(map(str, sqlDict.values())).strip('[*]'))
				self.rdbCur.execute(strSQL)

			else:
				row = self.rdbCur.fetchone()

				for key in row:
					if row[key] <> None and (data.get(key, None) == None or data[key] == ''):
						data[key] = row[key]

				strSQL = """DELETE FROM appsync WHERE deviceID = '%s' and pkgName = '%s'
                        """ % (data.get('deviceID', ''), data.get('pkgName', ''))
				self.rdbCur.execute(strSQL)

			self.updateAppssLst(data, sessionID)
			self.rdbCur.execute("COMMIT")
			return retFunc

		except Exception, e:
			self.rdbCur.execute("ROLLBACK")
			log.error("procSyncID:%s" % e)
			log.error(data)
			return 0

	def processLog(self, data):

		try:
			deviceId = data.get('deviceID')
			pkgnm = data.get('pkgName')

			curSID = int(data.get('sID', 0))
			curTTM = float(data['tTM'])
			curNumHits = int(data['numTotalHits'])

			strSQL = """SELECT tTM, numTotalHits, sID, sessionID
    FROM appsession_lst
    WHERE androidID = '%s' AND pkgnm = '%s'""" % (deviceId, pkgnm)
			ret = self.rdbCur.execute(strSQL)
			if ret > 0:
				row = self.rdbCur.fetchone()
				lstSID = int(row['sID'])
				lstTTM= float(row['tTM'])
				lstNumHits = int(row['numTotalHits'])
				lstSSID = row['sessionID']
			else:
				lstSID = 0
				lstTTM = 0.0
				lstNumHits = 0
				lstSSID = ""

			if (lstSID > curSID) or (lstSID == curSID and lstTTM > curTTM) \
					or (lstSID == curSID and lstTTM == curTTM and lstNumHits >= curNumHits):
				return

			if data.get('evtSyncID', None) <> None and data['evtSyncID'] > 0:
				if self.procSyncID(data, lstSSID) == 0:
					return

			# Event/Log에 따른 선처리
			if data.get('evtKey', '') == '':
				if data.get('startCode', None) == None and data.get('startTime', None) == None:
					return
				else:
					data = self.preprocessLog(data)
					#elif data.has_key('evtSyncID'):
					#return

			self.rdbCur.execute("START TRANSACTION")

			#if ansLogVer >= 3.0, process NW charac
			if data.get('ansLogVer', '') > '':
				dataVer = map(int, data['ansLogVer'].split('.'))
				if dataVer >= [3,0]:
					self.processNWcharac(data)

			strSQL = """SELECT m.*, IFNULL(b.sessionID, '') AS bsessionID, IFNULL(b.ntype, '') AS nt, IFNULL(b.stm, 0) AS bstm,
							IFNULL(b.etm, 0) AS betm, IFNULL(b.netid, '') AS bnetid, IFNULL(b.cellId, '') AS bcellid,
							IFNULL(b.actdur, 0) AS bactdur, IFNULL(b.actbytes, 0) AS bactbytes, IFNULL(b.exitCode, -99) AS bexitCode,
							IFNULL(b.myfion, -1) AS bmyfion, IFNULL(b.myfiap, -1) AS bmyfiap,
							IFNULL(c.sessionID, '') AS csessionID, IFNULL(c.ccstm, 0) AS ccstm, IFNULL(c.etm, '') AS cetm,
							IFNULL(c.cellId, '') AS ccellid, IFNULL(c.exitCode, -99) AS cexitCode
						FROM
						(SELECT
							sessionID, androidID, pkgnm, sID, `mode`,
							agentMode, ansVersion, statAppss,
							initialWifi, initialWifiOnCount, initialWifiOffCount,
							countryCode, operatorCode,
							startTime, endTime, startCode, exitCode,
							wifiPlayTime, mobilePlayTime, wifiConnectedDuration, mobileConnectedDuration,
							wifiTraffic, wifiActiveTraffic, mobileTraffic, mobileActiveTraffic,
							wifiTrafficBySys, wifiTrafficByMAO,
							wifiToMobileChangeCount, mobileToWifiChangeCount,
							beginLong, beginLat, lastLong, lastLat,
							apppkgnm, appvercd, parentappnm
						FROM appsession
						WHERE sessionID LIKE '%s%%' AND androidID = '%s' AND pkgnm = '%s'
						ORDER BY sessionID DESC LIMIT 1) m LEFT OUTER JOIN appsession_net b ON m.sessionID = b.sessionID
								LEFT OUTER JOIN appsession_netcell c ON m.sessionID = c.sessionID AND b.ntype = c.ntype AND b.stm = c.stm
						ORDER BY b.stm DESC, c.ccstm DESC LIMIT 1""" % (deviceId,deviceId, pkgnm)

			ret = self.rdbCur.execute(strSQL)
			if ret == 0:
				self.makeSession(data)
				self.rdbCur.execute("COMMIT")
				return

			session = self.rdbCur.fetchone()
			data['mode'] = session['mode']

			if session['endTime'] > int(curTTM):
				self.updateAppssLst(data, "")
				self.rdbCur.execute("COMMIT")
				return

			startCode = int(data.get('startCode', -99))
			evtCode = int(data.get('evtValue', -99))
			evtStartCode = startCode if startCode > 0 else evtCode

			if session['sID'] != curSID or evtCode in (10, 91, 92) or \
					(session['statAppss'] == '0' and (evtStartCode in (10, 30, 50, 70, 91, 92))) or \
					(session['mode'] == 1 and int(curTTM) - session['endTime'] > 305) or \
					(int(curTTM) - session['endTime'] > 605):
				#(session['statAppss'] == '2' and (data.get('startCode', -99) == 70 or data.get('evtValue', -99) == 70)) or  \
				self._closePrevAndMakeNewSession(session, data)
			else:
				self.updateSession(session, data)
				self.updateAppssLst(data, session['sessionID'])

			self.rdbCur.execute("COMMIT")

		except Exception, e:
			self.rdbCur.execute("ROLLBACK")
			log.error("AppSession.processLog [androidID:%s, sID:%s, tTM:%s, numTotalHits:%s] %s" % (data.get('deviceID', '-'), data.get('sID', -9), data.get('tTM', -9), data.get('numTotalHits', -9), e))
			raise e


	def clippingTP(self, traffic, duration):
		if duration == 0:
			return 0
		else:
			return traffic

	def processNWcharac(self, alog):

		try:
			if alog.get('bssid', '') == '' or alog['bssid'] == None:
				return

			bssid = alog['bssid']

			SumfrLst = [{'fq':0, 'avgrssi':0} for i in range(10)]
			SumTpDict = {}
			cocoaAvg = {'freq':0, 'estTP':0.0}
			tpAvg = {'freq':0, 'avgTP':0.0}

			#get saved nwcharac values from RDB
			strSQL = "SELECT itype, lvl, freq, fvalue FROM apnwc WHERE bssid = '%s'" % bssid
			ret = self.rdbCur.execute(strSQL)

			if ret > 0:
				rows = self.rdbCur.fetchall()
				for row in rows:
					if row['itype'] == '0':
						SumfrLst[row['lvl']]['avgrssi'] = row['fvalue']
						SumfrLst[row['lvl']]['fq'] = row['freq']

					elif row['itype'] == '1':
						SumTpDict[str(row['lvl'])] = row['freq']

					elif row['itype'] == '2':
						cocoaAvg = {'freq':row['freq'], 'estTP':row['fvalue']}

					elif row['itype'] == '3':
						tpAvg = {'freq':row['freq'], 'avgTP':row['fvalue']}

					else:
						log.warn("processNWcharac Unknow itype %s:%s" % (bssid, row['itype']))
						continue

			#parse aatlog's NWcharac string & store into temporary storage
			if alog.get('NWcharac', None) <> None and alog['NWcharac'] > '':
				nwstr = alog['NWcharac'].split('|')
				if len(nwstr) == 2:
					fqrssiLst = nwstr[0].split('.')
					for i, fqrssi in enumerate(fqrssiLst):
						fqlst = fqrssi.split(',')
						if len(fqlst) == 2 and fqlst[0].isdigit() and fqlst[0] > 0 and fqlst[1].isdigit():
							#RSSI AVG: frequency number * rssi / frequency number = (lastest) avg. rssi
							#RSSI AVG Accumulated: (lastest avg. rssi * sum of frequency number + new freq * rssi) / (sum of freq number + new freq)
							SumfrLst[i]['avgrssi'] = float(SumfrLst[i]['avgrssi']*SumfrLst[i]['fq'] + int(fqlst[0])*int(fqlst[1])) / float(SumfrLst[i]['fq']+int(fqlst[0]))
							SumfrLst[i]['fq'] += int(fqlst[0])


					varTPLst = nwstr[1].split('.')
					for varTP in varTPLst:
						TPLst = varTP.split(',')
						if len(TPLst) == 2 and TPLst[0].isdigit():
							if SumTpDict.has_key(TPLst[0]):
								SumTpDict[TPLst[0]] += 1
							else:
								SumTpDict[TPLst[0]] = 1


			if alog.has_key('estTP') and alog['estTP'] <> None:
				if alog['estTP'] > 0:
					cocoaAvg['freq'] += 1
					cocoaAvg['estTP'] += int(alog['estTP'])

			if alog.has_key('avgTP') and alog['avgTP'] <> None:
				if alog['avgTP'] > 0:
					tpAvg['freq'] += 1
					tpAvg['avgTP'] += alog['avgTP']

			#update RDB data
			valuesLst = []
			for i, fr in enumerate(SumfrLst):
				strvalue = "('%s', '0', %d, %d, %f)" % (bssid, i, fr['fq'], fr['avgrssi'])
				valuesLst.append(strvalue)

			for key in SumTpDict:
				strvalue = "('%s', '1', %d, %d, 0.0)" % (bssid, int(key), SumTpDict[key])
				valuesLst.append(strvalue)

			strvalue = "('%s', '2', 0, %d, %f)" % (bssid, cocoaAvg['freq'], cocoaAvg['estTP'])
			valuesLst.append(strvalue)

			strvalue = "('%s', '3', 0, %d, %f)" % (bssid, tpAvg['freq'], tpAvg['avgTP'])
			valuesLst.append(strvalue)

			strSQL = """INSERT INTO apnwc (bssid, itype, lvl, freq, fvalue)
                VALUES  %s
                ON DUPLICATE KEY UPDATE
                    freq = VALUES(freq), fvalue = VALUES(fvalue)
                """ % ', '.join(valuesLst)

			ret = self.rdbCur.execute(strSQL)

		except Exception, e:
			log.error("processNWcharac: %s" % e)
			log.error(alog)

	def passLog(self, devid):
		try:
			strSQL = """SELECT
				deviceID,	pkgName,	sID,		tTM,			objid,		agentAllowAns,		ansLogVer,		ansVer,			avgTP,		bssid,
				cellId, 	duration,	estTP,		evtKey,			evtValue,	evtSyncID,			startCode,		exitCode,		startTime,	eventTime,
				initWifi,	model,		netType,	numTotalHits,	NWcharac,	playAppPackageName,	playTime,		activeDownload,	activeTime, totalDownload,
				linkspd,	apfreq,		ver,		verCode,		myfiOn,		myfiAP,				parentAppName, 	insTs
				from appsslog where deviceID = '%s'
				order by deviceID, pkgName, sID, tTM""" % (devid)
			ret = self.rdbCur.execute(strSQL)

			log.info("passLog:appsslog ret = %d " % ret)

			if ret > 0:
				rows = self.rdbCur.fetchall()
				cutTs = time.time() - 5
				pdevid = 'NULL'
				ppkgname = 'NULL'
				psid = -9
				for row in rows:
					if pdevid == row['deviceID'] and ppkgname == row['pkgName'] and psid == row['sID']:
						log.info("passLog: pass %s %s %s" % (pdevid, ppkgname, psid))
						continue
					if row['insTs'] > cutTs:
						pdevid = row['deviceID']
						ppkgname = row['pkgName']
						psid = row['sID']
						log.info("passLog: insTs(%d) > cutTs(%d) %s %s %s" % (row['insTs'], cutTs, pdevid, ppkgname, psid))
						continue
					keepkeys = filter(lambda x: row[x] <> None, row)
					newrow = {k: row[k] for k in keepkeys}

				#	log.info("passLog: before processLog")
				#	log.info(newrow)

					self.processLog(newrow)
					strSQL = """DELETE from appsslog where deviceID = '%s' and pkgName = '%s' and sID = %d and tTM = %.3f""" % \
					         (newrow['deviceID'], newrow['pkgName'], newrow['sID'], newrow['tTM'])
					ret = self.rdbCur.execute(strSQL)
					log.info("passLog: delete appsslog %d (inTs:%.3f cutTs:%.3f) (%s, %s, %d, %.3f)" % (ret, newrow['insTs'], cutTs, newrow['deviceID'], newrow['pkgName'], newrow['sID'], newrow['tTM']))

			return ret

		except Exception, e:
			log.error("passLog : %s" % e)
			log.error(newrow)
			raise e

	def process(self, params):
		try:
			self.rdbCur = None
			self.rdbCur = worker.dbmanager.allocDictCursor('myapwave')

			pnm = "p%x" % (int(random.random()*100) % 15 + 1)

			while True:
				self.rdbCur.execute("START TRANSACTION")
				strSQL = "SELECT deviceID, lstuptmp from appssdist partition (%s) where isbusy = '0' and procTmp < %d order by lstuptmp limit 1 for update" % (pnm, params['tmp'])
				ret = self.rdbCur.execute(strSQL)
				if ret > 0 and ret < 10000: # Sometimes Mysql returns 18446744073709551615 instead of -1
					row = self.rdbCur.fetchone()
					devid = row['deviceID']
					lstuptmp = row['lstuptmp']
					strSQL = "UPDATE appssdist SET isbusy = '1' where deviceID = '%s'" % (devid)
					ret = self.rdbCur.execute(strSQL)
					self.rdbCur.execute("COMMIT")
					log.info("process:appssdist '1' %s" % devid)
					try:
						procnt = self.passLog(devid)
					except Exception, e:
						log.error("Appsession.process in while [%s] %s" % (devid, e))

					while True:
						try:
							rtcnt = 0
							if procnt > 0:
								strSQL = "UPDATE appssdist SET isbusy = '0', procTmp = %d, lstuptmp = unix_timestamp() where deviceID = '%s'" % (params['tmp'], devid)
							else:
								if lstuptmp < params['tmp'] - 600:
									strSQL = "DELETE FROM appssdist where deviceID = '%s'" % (devid)
								else:
									strSQL = "UPDATE appssdist SET isbusy = '0', procTmp = %d  where deviceID = '%s'" % (params['tmp'], devid)

							ret = self.rdbCur.execute(strSQL)
							log.info("process: appssdist '0' %s" % devid)
							break
						except Exception, e:
							if str(e).find('restart') > 0:
								log.warn("process: appssdist[%s] set to '0' retry: %d, %s" % (devid, rtcnt, e))
								if rtcnt > 10:
									raise e
								time.sleep(rtcnt+1)
								rtcnt += 1
							else:
								log.error("process: appssdist[%s] set to '0' %s" % (devid, e))
								raise e

				else:
					self.rdbCur.execute("COMMIT")
					log.info("process:appssdist No dev return")
					break


		except Exception, e:
			log.error("AppSession.process %s, [%s]" % (e, strSQL))
			log.error(params)

		finally:
			if self.rdbCur <> None:
				self.rdbCur.execute("COMMIT")
				worker.dbmanager.freeCursor(self.rdbCur)

# 전체 device 처리
class ProcessAnsClock(object):
	# static class variable
	OW_TASK_SUBSCRIBE_EVENTS = ['evtTimeInterval']
	OW_TASK_PUBLISH_EVENTS = []
	OW_USE_HASHING = False
	OW_HASH_KEY = None
	OW_NUM_WORKER = 16

	def publishEvent(self, event, params):
		# THIS METHOD WILL BE OVERRIDE
		# DO NOT EDIT THIS METHOD
		pass

	def handler(self, params):
		global appss
		global lastParamTmp

		try:
			log.info("evtTimeInterval get %d (last:%d)" % (params['tmp'], lastParamTmp))
			if params['tmp'] > lastParamTmp:
				log.info("appss.process start")
				appss.process(params)
				lastParamTmp = params['tmp']
				log.info("appss.process done")

		except Exception, e:
			log.error("processAnsClock ERROR %s" % (e) )

		return


if __name__ == "appss":
	appss = AppSession()
	random.seed(os.getpid())
