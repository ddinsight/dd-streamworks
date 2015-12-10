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
Not all aatLog items are included in Open DD.
You should ignore some items & tables in this module.
Items not relevant to Open DD are specified.
"""
__author__ = 'jaylee'

import time
from decimal import Decimal
import worker
from worker import log


# traffic mode
class TrafficMode:
	SYSTEM = 0
	AAT_AGENT = 1


# logType
class aatLogType:
	END = 0
	START = 1
	PERIOD = 2
	USER_PAUSE = 3
	USER_RESUME = 4
	NET_CHANGE = 5
	NET_PAUSE = 6
	NET_RESUME = 7
	NET_CHANGE_DURING_PAUSE = 8
	BITRATE_CHANGE = 9
	SEEK = 10
	UNKNOWN = -1


class VidnetType:
	VIDNET_TYPE_PLAY = 0
	VIDNET_TYPE_NET_PAUSE = 1
	VIDNET_TYPE_NET_RESUME = 2
	VIDNET_TYPE_USER_PAUSE = 3
	VIDNET_TYPE_USER_RESUME = 4
	VIDNET_TYPE_UNKNOWN = 5


class ExceptionType:
	NO_AATLOG = 1			  # AAT
	INVALID_LOG_PATTERN = 2	 # live
	INVALID_LOGTYPE_IN_VIDET = 3
	TRAFFIC_OVERFLOW = 4		# vidnet


class NetworkType:
	WIFI = 0
	CELLULAR = 1
	UNKNOWN = 2


#Following function is not related to Open DD. You should ignore it
def updateMcc(deviceID, plmnId):
	try:
		rcur = None
		strSQL = ""

		if plmnId.isdigit() == True and int(plmnId) > 0:
			rcur = worker.dbmanager.allocDictCursor('myapmain')
			strSQL = "UPDATE mdev SET plmnid = '%s' WHERE mosid = '%s' and (plmnid <= ' ' or plmnid is NULL)" % (plmnId, deviceID)
			ret = rcur.execute(strSQL)

	except Exception, e:
		log.error("updateMcc: %s" % e)
		log.error("updateMcc: [deviceID:%s, plmnid:%s, strSQL:%s]" % (deviceID, plmnId, strSQL))
	finally:
		if rcur <> None:
			worker.dbmanager.freeCursor(rcur)


def updateVidLog(waveCursor, vLog, row):
	try:
		strSQL = ""
		strSQL = """UPDATE vidsession_log
    SET playTime = if(playTime >= %d, playTime - %d, playTime),
        pauseTime = if(pauseTime >= %d, pauseTime - %d, pauseTime),
        elapsedTime = if(elapsedTime >= %d, elapsedTime - %d, elapsedTime),
        cellRxBytes = if(cellRxBytes >= %d, cellRxBytes - %d, cellRxBytes),
        wfRxBytes = if(wfRxBytes >= %d, wfRxBytes - %d, wfRxBytes),
        cellDuration = if(cellDuration >= %d, cellDuration - %d, cellDuration),
		wfDuration = if(wfDuration >= %d, wfDuration -%d, wfDuration),
		lstuptmp = unix_timestamp()
    WHERE playSessionID = '%s' and tTM = %s """ % (vLog['playTime'], vLog['playTime'],
		                                           vLog['pauseTime'], vLog['pauseTime'],
		                                           vLog['elapsedTime'], vLog['elapsedTime'],
		                                           vLog['cellRxBytes'], vLog['cellRxBytes'],
		                                           vLog['wfRxBytes'], vLog['wfRxBytes'],
		                                           vLog['cellDuration'], vLog['cellDuration'],
		                                           vLog['wfDuration'], vLog['wfDuration'],
		                                           vLog['playSessionID'], row['nextTTM'])
		waveCursor.execute(strSQL)

	except Exception, e:
		log.error("updateVidLog %s" % e)
		log.error(vLog)
		log.error(row)
		if strSQL > "":
			log.error("[SQL] %s" % strSQL)
		raise e


def getVidLogStatic(vidLogDict, aatLog, appSessionId, netType):
	vidLogDict['playSessionID'] = aatLog['playSessionId']
	vidLogDict['tTM'] = Decimal(aatLog['tTM'])
	vidLogDict['oid'] = aatLog.get('log_time', '')
	vidLogDict['appSessionID'] = appSessionId
	vidLogDict['logType'] = aatLog.get('agentLogType', -1)
	vidLogDict['logStartTime'] = aatLog.get('agentLogStartTime', 0)
	vidLogDict['logEndTime'] = aatLog.get('agentLogEndTime', 0)
	vidLogDict['cellid'] = "%s_%s_%s" % (aatLog.get('confOperator', ''), aatLog.get('netCID', ''),  aatLog.get('netLAC', ''))
	vidLogDict['ntype'] = netType
	vidLogDict['abrMode'] = aatLog.get('abrMode', '')
	if aatLog.has_key('requestBR'):
		vidLogDict['curBitrate'] = aatLog.get('liveCurrentTSBitrate', 0)
		vidLogDict['reqBitrate'] = aatLog['requestBR']
	else:
		vidLogDict['curBitrate'] = aatLog.get('reqBitrate', 0)
		vidLogDict['reqBitrate'] = aatLog.get('liveCurrentTSBitrate', 0)
	vidLogDict['bbCount'] = aatLog.get('bbCount', 0)
	vidLogDict['netCellState'] = aatLog.get('netCellState', '')
	vidLogDict['bufferState'] = aatLog.get('playBufferState', '0')
	vidLogDict['cellSysRxBytes'] = aatLog.get('trafficSystemMoRxBytes', 0)
	vidLogDict['wfSysRxBytes'] = aatLog.get('trafficSystemWFRxBytes', 0)
	vidLogDict['playEndState'] = aatLog.get('playEndState', '')
	strNetActive = aatLog.get('netActiveNetwork', '')
	if strNetActive.upper().find('WIFI') > 0:
		strtokens = strNetActive.split('|')
		if len(strtokens) > 5:
			vidLogDict['ssid'] = strtokens[3]
			vidLogDict['bssid'] = strtokens[4]


def vidupdate(waveCursor, aatLog, row):
	try:
		vidDict = {}
		vidLogDict = {}

		#get some values to use
		cellid = "%s_%s_%s" % (aatLog.get('confOperator', ''), aatLog.get('netCID', ''),  aatLog.get('netLAC', ''))
		psmode = int(aatLog['playServiceMode'])
		logType = aatLog.get('agentLogType', -1)
		if aatLog.get('netActiveNetwork', '').find('WIFI') >= 0:
			netType = '0'
		elif aatLog.get('netActiveNetwork', '').find('mobile') >= 0:
			netType = '1'
		else:
			netType = '2'

		batteryStart = 0
		batteryEnd = 0
		batteryValid = 0
		if aatLog.has_key('batteryInfo'):
			btList = aatLog['batteryInfo'].split('|')
			if len(btList) == 2:
				if len(btList[0].split('/')) >= 5 and len(btList[1].split('/')) >= 5:
					nTotLevel = float(btList[0].split('/')[3])
					nBatLevel = float(btList[0].split('/')[4])
					batteryStart = (nBatLevel/nTotLevel)*100
					nTotLevel = float(btList[1].split('/')[3])
					nBatLevel = float(btList[1].split('/')[4])
					batteryEnd = (nBatLevel/nTotLevel)*100
					if btList[1].split('/')[1] == 'DISCHARGING':  #All batteryInfo reporting log must be 'DISCHARGING' except first.
						batteryValid = 1
					else:
						batteryValid = 0
			elif len(btList) == 1:
				if len(btList[0].split('/')) >= 5:
					nTotLevel = float(btList[0].split('/')[3])
					nBatLevel = float(btList[0].split('/')[4])
					batteryStart = (nBatLevel/nTotLevel)*100
					batteryEnd = batteryStart
					batteryValid = 0

		#get appSessionID
		appSessionId = ''
		strSQL = """SELECT sessionID FROM appsession WHERE androidID = '%s' and pkgnm = '%s' and sID = %d
			and (startTime - 5) <= %d and startTime > 0 and (endTime > %d or statAppss > '0') ORDER BY sessionID DESC LIMIT 1""" % (aatLog['deviceID'],
		                                                                                                                      aatLog['pkgName'], aatLog['sID'], aatLog['agentLogStartTime'], aatLog['agentLogStartTime'])
		ret = waveCursor.execute(strSQL)
		if ret > 0:
			aarow = waveCursor.fetchone()
			if aarow['sessionID'] > '':
				appSessionId = aarow['sessionID']

		#vidsession_log values
		getVidLogStatic(vidLogDict, aatLog, appSessionId, netType)

		#initialize if as-is record has no valid value.
		if row['androidID'] == '': vidDict['androidID'] = aatLog.get('deviceID', '')
		if row['vID'] == '': vidDict['vID'] = aatLog.get('vID', '')
		if row['sID'] == 0: vidDict['sID'] = aatLog.get('sID', 0)
		if row['verCode'] == 0: vidDict['verCode'] = aatLog.get('verCode', 0)
		if row['osVer'] == '': vidDict['osVer'] = aatLog.get('osVer', '')
		if row['brand'] == '': vidDict['brand'] = aatLog.get('brand', '')
		if row['model'] == '': vidDict['model'] = aatLog.get('model', '')
		if row['cellIdSt'] == '' and len(cellid) > 6: vidDict['cellIdSt'] = cellid
		if row['cellIdEnd'] == '' and len(cellid) > 6: vidDict['cellIdEnd'] = cellid
		if row['bMao'] < 0:  vidDict['bMao'] = int(aatLog.get('agentAatOnOff', -1))
		if row['bAnsAllow'] < 0: vidDict['bAnsAllow'] = int(aatLog.get('agentAllowAns', -1))
		if row['bCellAllow'] < 0: vidDict['bCellAllow'] = int(aatLog.get('agentAllowMobile', -1))
		if row['ansMode'] == '': vidDict['ansMode'] = aatLog.get('agentAnsMode', -1)
		if row['agentUserSetup'] == '': vidDict['agentUserSetup'] = aatLog.get('agentUserSetup', '')
		#if row['startLogType'] == '': vidDict['ansMode'] = aatLog.get('agentAnsMode', -1)
		if row['hostName'] == '': vidDict['hostName'] = aatLog.get('playHost', '')
		if row['originName'] == '': vidDict['originName'] = aatLog.get('playOrigin', '')
		if row['contentID'] == '': vidDict['contentID'] = aatLog.get('playContentId', '')
		if row['playServiceMode'] <= 0: vidDict['playServiceMode'] = aatLog.get('playServiceMode', 0)
		if row['contentSize'] == 0:
			if psmode == 1:
				vidDict['contentSize'] = aatLog.get('vodContentSize', 0)
			elif psmode == 4:
				vidDict['contentSize'] = aatLog.get('audContentSize', 0)
			elif psmode == 5:
				vidDict['contentSize'] = aatLog.get('adnContentSize', 0)
		if row['contentDuration'] == 0:
			if psmode == 1:
				vidDict['contentDuration'] = aatLog.get('vodContentDuration', 0)
			elif psmode == 4:
				vidDict['contentDuration'] = aatLog.get('audContentDuration', 0)
		if row['contentBitrate'] == 0 and psmode in [2,3]:
			vidDict['contentBitrate'] = aatLog.get('liveCurrentTSBitrate', 0)
		#if row['channelName'] == '': vidDict['channelName'] = aatLog.get('playTitle', '').encode('utf-8')
		if row['channelName'] == '': vidDict['channelName'] = aatLog.get('playTitle', '')
		if row['pkgnm'] == '': vidDict['pkgnm'] = aatLog.get('pkgName', '')
		if row['apppkgnm'] == '' or row['appvercd'] == '':
			if(aatLog.has_key('playAppPackageName')):
				appPkgs = aatLog['playAppPackageName'].split('/')
				if len(appPkgs) >= 2:
					vidDict['apppkgnm'] = appPkgs[0]
					vidDict['appvercd'] = appPkgs[1]
		if row['connectedNetCnt'] == 0:
			if aatLog.has_key('netConnectedNetworkCount'):
				vidDict['connectedNetCnt']=aatLog['netConnectedNetworkCount']
			elif aatLog.has_key('netConnectivityCount'):
				vidDict['connectedNetCnt']=aatLog['netConnectivityCount']

		if row['abrBitrateList'] == '': vidDict['abrBitrateList'] = aatLog.get('playBitrateList', '')
		if row['abrUserSelBR'] == '': vidDict['abrUserSelBR'] = aatLog.get('userSelectBitrate', '')

		if psmode == 5:
			if row['vidnetType'] == 0: vidDict['vidnetType'] = aatLog.get('adnStartCode', 0)
			if row['adnMode'] == '' or (row['adnMode'] <> 'BB' and aatLog.get('adnMode', '') == 'BB') :
				vidDict['adnMode'] = aatLog.get('adnMode', '')
			if row['adnRangeStart'] == 0: vidDict['adnRangeStart'] = aatLog.get('adnContentRangeStart', 0)
			if row['adnDownSize'] < aatLog.get('adnDownloadSize', 0):
				vidDict['adnDownSize'] = aatLog.get('adnDownloadSize', 0)
			if row['contentDuration'] < aatLog.get('adnDownloadTime', 0):
				vidDict['contentDuration'] = aatLog.get('adnDownloadTime', 0)
			if row['adnContentID'] == 0: vidDict['adnContentID'] = aatLog.get('adnContentID', 0)


		vidDict['cellSysRxBytes'] = row['cellSysRxBytes'] + aatLog.get('trafficSystemMoRxBytes', 0)
		vidDict['wfSysRxBytes'] = row['wfSysRxBytes'] + aatLog.get('trafficSystemWFRxBytes', 0)

		# process attributes depending on log-order
		if aatLog['tTM'] > row['maxTTM']: 			#The log is the last of this playSession
			if len(cellid) > 6:
				vidDict['cellIdEnd'] = cellid
			vidDict['endLogType'] = logType
			vidDict['vidnetEndTime'] = aatLog.get('agentLogEndTime', 0)
			vidDict['vidnetDuration'] = vidDict['vidnetEndTime'] - row['vidnetStartTime']
			if aatLog.get('playPlayingTime', 0) > row['playTime']:
				vidDict['playTime'] = aatLog['playPlayingTime']
			if aatLog.get('playSeekCount', 0) > row['seekCnt']:
				vidDict['seekCnt'] = aatLog['playSeekCount']
			if aatLog.get('playSeekForwardCount', 0) > row['ffCnt']:
				vidDict['ffCnt'] = aatLog['playSeekForwardCount']
			if aatLog.get('playSeekRewindCount', 0) > row['rwCnt']:
				vidDict['rwCnt'] = aatLog['playSeekRewindCount']
			if aatLog.has_key('netConnectedNetworkCount'):
				if row['connectedNetCnt'] < aatLog['netConnectedNetworkCount']:
					vidDict['connectedNetCnt']=aatLog['netConnectedNetworkCount']
			elif aatLog.has_key('netConnectivityCount'):
				if row['connectedNetCnt'] < aatLog['netConnectivityCount']:
					vidDict['connectedNetCnt']=aatLog['netConnectivityCount']
			if psmode in [1, 4, 5]:
				vidDict['pauseCnt'] = aatLog.get('playBufferingCount', 0)
				vidDict['resumeCnt'] = aatLog.get('playResumeCount', 0)
			if aatLog.get('playAccBufferingTime', 0) > row['pauseTime']:
				vidDict['pauseTime'] = aatLog.get('playAccBufferingTime', 0)
			if aatLog.get('playMaxBufferingTime', 0) > row['maxPauseTime']:
				vidDict['maxPauseTime'] = aatLog.get('playMaxBufferingTime', 0)
			if aatLog.get('trafficAgentMoBytes', 0) > row['cellRxBytes']:
				vidDict['cellRxBytes'] = aatLog['trafficAgentMoBytes']
			if aatLog.get('trafficAgentWFBytes', 0) > row['wfRxBytes']:
				vidDict['wfRxBytes'] = aatLog['trafficAgentWFBytes']
			vidDict['cellAvgTP'] = round(aatLog.get('trafficAgentMoAveBW',0), 4)
			vidDict['wfAvgTP'] = round(aatLog.get('trafficAgentWFAveBW',0), 4)
			if vidDict['cellAvgTP'] > 0:
				vidDict['cellDuration'] = int((aatLog.get('trafficAgentMoBytes', 0)*8) / (aatLog['trafficAgentMoAveBW']*1000000))
			if vidDict['wfAvgTP'] > 0:
				vidDict['wfDuration'] = int((aatLog.get('trafficAgentWFBytes', 0)*8) / (aatLog['trafficAgentWFAveBW']*1000000))
			vidDict['batteryEnd'] = batteryEnd

			#get appSessionID for vidsession
			strSQL = """SELECT sessionID FROM appsession WHERE androidID = '%s' and pkgnm = '%s' and sID = %d
				and startTime < %d and startTime > 0 and ((endTime+5) > %d or statAppss > '0') ORDER BY sessionID DESC LIMIT 1""" % (aatLog['deviceID'],
			                                                                                                                      aatLog['pkgName'], aatLog['sID'], aatLog['agentLogEndTime'], aatLog['agentLogEndTime'])
			ret = waveCursor.execute(strSQL)
			if ret > 0:
				aarow = waveCursor.fetchone()
				if aarow['sessionID'] > '':
					vidDict['appSessionIDEnd'] = aarow['sessionID']

			#vidsession_log values
			if aatLog.get('playPlayingTime', 0) > row['playTime']:
				vidLogDict['playTime'] = aatLog.get('playPlayingTime', 0) - row['playTime']
			else:
				vidLogDict['playTime'] = 0
			if aatLog.get('playAccBufferingTime', 0) > row['pauseTime']:
				vidLogDict['pauseTime'] = aatLog.get('playAccBufferingTime', 0) - row['pauseTime']
			else:
				vidLogDict['pauseTime'] = 0
			if aatLog.get('playPreparingTime', 0) > row['elapsedTime']:
				vidLogDict['elapsedTime'] = aatLog.get('playPreparingTime', 0) - row['elapsedTime']
			else:
				vidLogDict['elapsedTime'] = 0
			if aatLog.get('trafficAgentMoBytes', 0) > row['cellRxBytes']:
				vidLogDict['cellRxBytes'] = aatLog.get('trafficAgentMoBytes', 0) - row['cellRxBytes']
			else:
				vidLogDict['cellRxBytes'] = 0
			if aatLog.get('trafficAgentWFBytes', 0) > row['wfRxBytes']:
				vidLogDict['wfRxBytes'] = aatLog.get('trafficAgentWFBytes', 0) - row['wfRxBytes']
			else:
				vidLogDict['wfRxBytes'] = 0
			if vidDict['cellAvgTP'] > 0 and vidDict['cellDuration'] > row['cellDuration']:
				vidLogDict['cellDuration'] = vidDict['cellDuration'] - row['cellDuration']
			else:
				vidLogDict['cellDuration'] = 0
			if vidDict['wfAvgTP'] > 0 and vidDict['wfDuration'] > row['wfDuration']:
				vidLogDict['wfDuration'] = vidDict['wfDuration'] - row['wfDuration']
			else:
				vidLogDict['wfDuration'] = 0


		elif row['bpsid'] == '':		# The log is the first of this playSession
			if len(cellid) > 6:
				vidDict['cellIdSt'] = cellid
			vidDict['startLogType'] = logType
			vidDict['vidnetStartTime'] = aatLog.get('agentLogStartTime', 0)
			vidDict['vidnetDuration'] = row['vidnetEndTime'] - vidDict['vidnetStartTime']
			vidDict['batteryStart'] = batteryStart

			if appSessionId > '':
				vidDict['appSessionIDSt'] = appSessionId

			#vidsession_log values
			vidLogDict['playTime'] = aatLog.get('playPlayingTime', 0)
			vidLogDict['pauseTime'] = aatLog.get('playAccBufferingTime', 0)
			vidLogDict['elapsedTime'] = aatLog.get('playPreparingTime', 0)
			vidLogDict['cellRxBytes'] = aatLog.get('trafficAgentMoBytes', 0)
			vidLogDict['wfRxBytes'] = aatLog.get('trafficAgentWFBytes', 0)
			if round(aatLog.get('trafficAgentMoAveBW',0), 4) > 0:
				vidLogDict['cellDuration'] = int((aatLog.get('trafficAgentMoBytes', 0)*8) / (aatLog['trafficAgentMoAveBW']*1000000))
			else:
				vidLogDict['cellDuration'] = 0
			if round(aatLog.get('trafficAgentWFAveBW',0), 4) > 0:
				vidLogDict['wfDuration'] = int((aatLog.get('trafficAgentWFBytes', 0)*8) / (aatLog['trafficAgentWFAveBW']*1000000))
			else:
				vidLogDict['wfDuration'] = 0

			updateVidLog(waveCursor, vidLogDict, row)

		else: 								# The log is middle of this playSession
			if aatLog.get('playPlayingTime', 0) > row['mPlayTime']:
				vidLogDict['playTime'] = aatLog.get('playPlayingTime', 0) - row['mPlayTime']
			else:
				vidLogDict['playTime'] = 0
			if aatLog.get('playAccBufferingTime', 0) > row['mPauseTime']:
				vidLogDict['pauseTime'] = aatLog.get('playAccBufferingTime', 0) - row['mPauseTime']
			else:
				vidLogDict['pauseTime'] = 0
			if aatLog.get('playPreparingTime', 0) > row['mElapsedTime']:
				vidLogDict['elapsedTime'] = aatLog.get('playPreparingTime', 0) - row['mElapsedTime']
			else:
				vidLogDict['elapsedTime'] = 0
			if aatLog.get('trafficAgentMoBytes', 0) > row['mCellBytes']:
				vidLogDict['cellRxBytes'] = aatLog.get('trafficAgentMoBytes', 0) - row['mCellBytes']
			else:
				vidLogDict['cellRxBytes'] = 0
			if aatLog.get('trafficAgentWFBytes', 0) > row['mWFBytes']:
				vidLogDict['wfRxBytes'] = aatLog.get('trafficAgentWFBytes', 0) - row['mWFBytes']
			else:
				vidLogDict['wfRxBytes'] = 0

			vidLogDict['cellDuration'] = 0
			vidLogDict['wfDuration'] = 0
			if round(aatLog.get('trafficAgentMoAveBW',0), 4) > 0:
				tempdur = int((aatLog.get('trafficAgentMoBytes', 0)*8) / (aatLog['trafficAgentMoAveBW']*1000000))
				if tempdur > row['mCellDur']:
					vidLogDict['cellDuration'] = tempdur - int(row['mCellDur'])
			if round(aatLog.get('trafficAgentWFAveBW',0), 4) > 0:
				tempdur = int((aatLog.get('trafficAgentWFBytes', 0)*8) / (aatLog['trafficAgentWFAveBW']*1000000))
				if tempdur > row['mWFDur']:
					vidLogDict['wfDuration'] = tempdur - int(row['mWFDur'])

			updateVidLog(waveCursor, vidLogDict, row)


		# process independent attributes not depending on log-order
		if psmode in [2, 3]:
			if logType == 6:
				vidDict['pauseCnt'] = row['pauseCnt'] + 1
			elif logType == 7:
				vidDict['resumeCnt'] = row['resumeCnt'] + 1
		if logType in [5, 8]:
			if netType == '0':  		#WIFI
				vidDict['netW2CTransferCnt'] = row['netW2CTransferCnt'] + 1
			elif netType == '1':		#mobile
				vidDict['netC2WTransferCnt'] = row['netC2WTransferCnt'] + 1
		if row['batteryValid'] == '1' and batteryValid == 0:
			vidDict['batteryValid'] = '0'

		if aatLog.get('netCellState', -1) > 0 and row['netAllowCell'] == '1':  	#The log not allow cell, as-is fully allowed.
			vidDict['netAllowCell'] = '2'
		elif aatLog.get('netCellState', -1) == 0 and row['netAllowCell'] == '0':  	#The log allow cell, as-is not allowed at all.
			vidDict['netAllowCell'] = '2'

		vidDict['bbCount'] = row['bbCount'] + aatLog.get('bbCount', 0)

		if row['elapsedTime'] == 0 and aatLog.get('playPreparingTime', 0) > 0:
			vidDict['elapsedTime'] = aatLog['playPreparingTime']
		elif row['mLogType'] == 10 and row['mBufferState'] == '2' and aatLog.get('playPreparingTime', 0) > 0:
			vidDict['elapsedTime'] = row['elapsedTime'] + aatLog['playPreparingTime']

		#insert tables
		vidDict['playSessionID'] = row['playSessionID']
		updateVidnet(waveCursor, vidDict)

		insertVidnetLog(waveCursor, vidLogDict)
	except Exception, e:
		log.error("vidupdate %s" % e)
		log.error(aatLog)
		log.error(vidDict)
		raise e


def vidcreate(waveCursor, aatLog):
	try:

		vidDict = {}
		vidLogDict = {}

		#get some values to use
		cellid = "%s_%s_%s" % (aatLog.get('confOperator', ''), aatLog.get('netCID', ''),  aatLog.get('netLAC', ''))
		psmode = int(aatLog['playServiceMode'])
		logType = aatLog.get('agentLogType', -1)
		if aatLog.get('netActiveNetwork', '').find('WIFI') >= 0:
			netType = '0'
		elif aatLog.get('netActiveNetwork', '').find('mobile') >= 0:
			netType = '1'
		else:
			netType = '2'

		vidDict['playSessionID'] = aatLog['playSessionId']
		vidDict['androidID'] = aatLog.get('deviceID', '')
		vidDict['vID'] = aatLog.get('vID', '')
		vidDict['sID'] = aatLog.get('sID', 0)
		vidDict['verCode'] = aatLog.get('verCode', 0)
		vidDict['osVer'] = aatLog.get('osVer', '')
		vidDict['brand'] = aatLog.get('brand', '')
		vidDict['model'] = aatLog.get('model', '')
		vidDict['cellIdSt'] = cellid
		vidDict['cellIdEnd'] = cellid
		vidDict['bMao'] = int(aatLog.get('agentAatOnOff', -1))
		vidDict['bAnsAllow'] = int(aatLog.get('agentAllowAns', -1))
		vidDict['bCellAllow'] = int(aatLog.get('agentAllowMobile', -1))
		vidDict['ansMode'] = aatLog.get('agentAnsMode', -1)
		vidDict['agentUserSetup'] = aatLog.get('agentUserSetup', '')
		#vidDict['ansMode'] = aatLog.get('agentAnsMode', -1)
		vidDict['hostName'] = aatLog.get('playHost', '')
		vidDict['originName'] = aatLog.get('playOrigin', '')
		vidDict['contentID'] = aatLog.get('playContentId', '')
		vidDict['playServiceMode'] = psmode

		if psmode == 1:
			vidDict['contentSize'] = aatLog.get('vodContentSize', 0)
		elif psmode == 4:
			vidDict['contentSize'] = aatLog.get('audContentSize', 0)
		elif psmode == 5:
			vidDict['contentSize'] = aatLog.get('adnContentSize', 0)
		else:
			vidDict['contentSize'] = 0

		if psmode == 1:
			vidDict['contentDuration'] = aatLog.get('vodContentDuration', 0)
		elif psmode == 4:
			vidDict['contentDuration'] = aatLog.get('audContentDuration', 0)
		elif psmode == 5:
			vidDict['contentDuration'] = aatLog.get('adnDownloadTime', 0)
		else:
			vidDict['contentDuration'] = 0

		if psmode in [2,3]:
			vidDict['contentBitrate'] = aatLog.get('liveCurrentTSBitrate', 0)
		else:
			vidDict['contentBitrate'] = 0

		#vidDict['channelName'] = aatLog.get('playTitle', '').encode('utf-8')
		vidDict['channelName'] = aatLog.get('playTitle', '')
		vidDict['pkgnm'] = aatLog.get('pkgName', '')
		vidDict['apppkgnm'] = ""
		vidDict['appvercd'] = ""
		if(aatLog.has_key('playAppPackageName')):
			appPkgs = aatLog['playAppPackageName'].split('/')
			if len(appPkgs) >= 2:
				vidDict['apppkgnm'] = appPkgs[0]
				vidDict['appvercd'] = appPkgs[1]
		if aatLog.has_key('netConnectedNetworkCount'):
			vidDict['connectedNetCnt']=aatLog['netConnectedNetworkCount']
		elif aatLog.has_key('netConnectivityCount'):
			vidDict['connectedNetCnt']=aatLog['netConnectivityCount']
		else:
			vidDict['connectedNetCnt']=0

		vidDict['abrBitrateList'] = aatLog.get('playBitrateList', '')
		vidDict['abrUserSelBR'] = aatLog.get('userSelectBitrate', '')

		if psmode == 5:
			vidDict['vidnetType'] = aatLog.get('adnStartCode', 0)
			vidDict['adnMode'] = aatLog.get('adnMode', '')
			vidDict['adnRangeStart'] = aatLog.get('adnContentRangeStart', 0)
			vidDict['adnDownSize'] = aatLog.get('adnDownloadSize', 0)
			vidDict['adnContentID'] = aatLog.get('adnContentID', 0)

		vidDict['startLogType'] = logType
		vidDict['endLogType'] = logType
		vidDict['vidnetStartTime'] = aatLog.get('agentLogStartTime', 0)
		vidDict['vidnetEndTime'] = aatLog.get('agentLogEndTime', 0)
		vidDict['vidnetDuration'] = vidDict['vidnetEndTime'] - vidDict['vidnetStartTime']

		# process independent attributes not depending on log-order
		vidDict['pauseCnt'] = 0
		vidDict['resumeCnt'] = 0
		vidDict['netW2CTransferCnt'] = 0
		vidDict['netC2WTransferCnt'] = 0

		if psmode in [2, 3]:
			if logType == 6:
				vidDict['pauseCnt'] = 1
			elif logType == 7:
				vidDict['resumeCnt'] = 1
		elif psmode in [1, 4, 5]:
			vidDict['pauseCnt'] = aatLog.get('playBufferingCount', 0)
			vidDict['resumeCnt'] = aatLog.get('playResumeCount', 0)

		if logType in [5, 8]:
			if netType == '0':  		#WIFI
				vidDict['netW2CTransferCnt'] = 1
			elif netType == '1':		#mobile
				vidDict['netC2WTransferCnt'] = 1

		vidDict['playTime'] = aatLog.get('playPlayingTime', 0)
		vidDict['seekCnt'] = aatLog.get('playSeekCount', 0)
		vidDict['ffCnt'] = aatLog.get('playSeekForwardCount', 0)
		vidDict['rwCnt'] = aatLog.get('playSeekRewindCount', 0)
		vidDict['pauseTime'] = aatLog.get('playAccBufferingTime', 0)
		vidDict['maxPauseTime'] = aatLog.get('playMaxBufferingTime', 0)
		vidDict['cellRxBytes'] = aatLog.get('trafficAgentMoBytes', 0)
		vidDict['wfRxBytes'] = aatLog.get('trafficAgentWFBytes', 0)
		vidDict['cellAvgTP'] = round(aatLog.get('trafficAgentMoAveBW',0), 4)
		vidDict['wfAvgTP'] = round(aatLog.get('trafficAgentWFAveBW',0), 4)
		vidDict['cellDuration'] = 0
		vidDict['wfDuration'] = 0
		vidDict['cellSysRxBytes'] = aatLog.get('trafficSystemMoRxBytes', 0)
		vidDict['wfSysRxBytes'] = aatLog.get('trafficSystemWFRxBytes', 0)
		if vidDict['cellAvgTP'] > 0:
			vidDict['cellDuration'] = int((aatLog.get('trafficAgentMoBytes', 0)*8) / (aatLog['trafficAgentMoAveBW']*1000000))
		if vidDict['wfAvgTP'] > 0:
			vidDict['wfDuration'] = int((aatLog.get('trafficAgentWFBytes', 0)*8) / (aatLog['trafficAgentWFAveBW']*1000000))

		batteryStart = 0
		batteryEnd = 0
		batteryValid = '0'
		if aatLog.has_key('batteryInfo'):
			btList = aatLog['batteryInfo'].split('|')
			if len(btList) == 2:
				if len(btList[0].split('/')) >= 5 and len(btList[1].split('/')) >= 5:
					nTotLevel = float(btList[0].split('/')[3])
					nBatLevel = float(btList[0].split('/')[4])
					batteryStart = (nBatLevel/nTotLevel)*100
					nTotLevel = float(btList[1].split('/')[3])
					nBatLevel = float(btList[1].split('/')[4])
					batteryEnd = (nBatLevel/nTotLevel)*100
					if btList[1].split('/')[1] == 'DISCHARGING':  #All batteryInfo reporting log must be 'DISCHARGING' except first.
						batteryValid = 1
					else:
						batteryValid = 0
			elif len(btList) == 1:
				if len(btList[0].split('/')) >= 5:
					nTotLevel = float(btList[0].split('/')[3])
					nBatLevel = float(btList[0].split('/')[4])
					batteryStart = (nBatLevel/nTotLevel)*100
					batteryEnd = batteryStart
					batteryValid = 0
		vidDict['batteryStart'] = batteryStart
		vidDict['batteryEnd'] = batteryEnd
		vidDict['batteryValid'] = str(batteryValid)

		if aatLog.get('netCellState', -1) > 0:
			vidDict['netAllowCell'] = '0'
		elif aatLog.get('netCellState', -1) == 0:
			vidDict['netAllowCell'] = '1'

		vidDict['bbCount'] = aatLog.get('bbCount', 0)
		vidDict['elapsedTime'] = aatLog.get('playPreparingTime', 0)

		#get appSessionID
		vidDict['appSessionIDSt'] = ''
		vidDict['appSessionIDEnd'] = ''
		strSQL = """SELECT MAX(1) as ord, MAX(sessionID) as sessionID FROM appsession WHERE androidID = '%s' and pkgnm = '%s' and sID = %d
			and (startTime - 5) <= %d and startTime > 0 and (endTime > %d or statAppss > '0')
            UNION ALL
            SELECT MAX(2), MAX(sessionID) FROM appsession WHERE androidID = '%s' and pkgnm = '%s' and sID = %d
			and startTime < %d and startTime > 0 and ((endTime + 5) > %d or statAppss > '0')
        """ % (aatLog['deviceID'], aatLog['pkgName'], aatLog['sID'], aatLog['agentLogStartTime'], aatLog['agentLogStartTime'],
		       aatLog['deviceID'], aatLog['pkgName'], aatLog['sID'], aatLog['agentLogEndTime'], aatLog['agentLogEndTime'])
		ret = waveCursor.execute(strSQL)
		if ret > 0:
			aarows = waveCursor.fetchall()
			for r in aarows:
				if r['sessionID'] > '' and r['sessionID'] <> None:
					if r['ord'] == 1:
						vidDict['appSessionIDSt'] = r['sessionID']
					elif r['ord'] == 2:
						vidDict['appSessionIDEnd'] = r['sessionID']

		#vidsession_log values
		getVidLogStatic(vidLogDict, aatLog, vidDict['appSessionIDSt'], netType)

		vidLogDict['playTime'] = aatLog.get('playPlayingTime', 0)
		vidLogDict['pauseTime'] = aatLog.get('playAccBufferingTime', 0)
		vidLogDict['elapsedTime'] = aatLog.get('playPreparingTime', 0)
		vidLogDict['cellRxBytes'] = aatLog.get('trafficAgentMoBytes', 0)
		vidLogDict['wfRxBytes'] = aatLog.get('trafficAgentWFBytes', 0)
		vidLogDict['cellSysRxBytes'] = aatLog.get('trafficSystemMoRxBytes', 0)
		vidLogDict['wfSysRxBytes'] = aatLog.get('trafficSystemWFRxBytes', 0)

		vidLogDict['cellDuration'] = 0
		vidLogDict['wfDuration'] = 0
		if round(aatLog.get('trafficAgentMoAveBW',0), 4) > 0:
			vidLogDict['cellDuration'] = int((aatLog.get('trafficAgentMoBytes', 0)*8) / (aatLog['trafficAgentMoAveBW']*1000000))
		if round(aatLog.get('trafficAgentWFAveBW',0), 4) > 0:
			vidLogDict['wfDuration'] = int((aatLog.get('trafficAgentWFBytes', 0)*8) / (aatLog['trafficAgentWFAveBW']*1000000))

		#insert tables
		insertVidnet(waveCursor, vidDict)
		insertVidnetLog(waveCursor, vidLogDict)

	except Exception, e:
		log.error("vidcreate %s" % e)
		log.error(aatLog)
		raise e



#Following function is not related to Open DD. You should ignore it
def getBBinfo(rBBList, aatLog):

	try:
		if aatLog.has_key('bbCount') == False or aatLog.has_key('bbList') == False:
			return

		BBcount = aatLog['bbCount']

		if BBcount == 0:
			return
		elif BBcount > 40:
			BBcount = 40

		if isinstance(aatLog['bbList'], list):
			bblst = aatLog['bbList'][0:BBcount]
		else:
			bblst = aatLog['bbList'].strip('[ ]').split(',')
			bblst = bblst[0:BBcount]

		bblst = bblst[0:BBcount]
		bbdict = {}

		for bbItem in bblst:
			if bbItem.find('|') < 0: continue
			bbElm = bbItem.strip(" u'\"").split('|')
			bbdict['psid'] = aatLog['playSessionId']
			bbdict['bb'] = list(bbElm)
			rBBList.append(bbdict.copy())

	except Exception, e:
		log.error("getBBinfo error:%s" % e)
		log.error(aatLog)
		raise e


#Following function is not related to Open DD. You should ignore it
def insertBBSQL(waveCursor, bbList):
	try:
		strLst = []
		for bbElm in bbList:
			if len(bbElm['bb']) == 8 and bbElm['bb'][7] == 'e':
				strValue = "('%s', %s, %s, '%s', '%s', %s, %s, %s, unix_timestamp())"  % (bbElm['psid'], bbElm['bb'][0], bbElm['bb'][2], \
				                                                        '{0:02d}'.format(int(bbElm['bb'][1])), '{0:02d}'.format(int(bbElm['bb'][3])),	bbElm['bb'][4], bbElm['bb'][5], bbElm['bb'][6])
			elif len(bbElm['bb']) == 6:
				strValue = "('%s', %s, %s, '%s', '%s', %s, %s, NULL, unix_timestamp())"  % (bbElm['psid'], bbElm['bb'][0], bbElm['bb'][2], \
				                                                          '{0:02d}'.format(int(bbElm['bb'][1])), '{0:02d}'.format(int(bbElm['bb'][3])), bbElm['bb'][4], bbElm['bb'][5])
			else:
				log.warn("BBList format error:")
				log.warn(bbElm)
				continue
			strLst.append(strValue)

		if len(strLst) > 0:
			sql = """insert into vidsession_bb (playSessionID, stTime, endTime, stCode, endCode, trWF, trCell, stBBTime, lstuptmp)
                     values %s
 					 on duplicate key update endTime = values(endTime), stCode = values(stCode), trWF = values(trWF), trCell = values(trCell), stBBTime = values(stBBTime), lstuptmp = unix_timestamp()
                  """ % ', '.join(strLst)

			ret = waveCursor.execute(sql)
			if ret == 0:
				log.warn("insertBBSQL no record affected [%s]" % sql)

	except Exception, e:
		log.error("insertBBSQL error:%s" % e)
		log.error(bbList)
		raise e

#Following function is not related to Open DD. You should ignore it
def getNetConn(rstNetList, aatLog):

	try:
		if aatLog.has_key('netConnectivityList') == False or len(aatLog['netConnectivityList']) == 0:
			return

		try:
#			log.info(type(aatLog['netConnectivityList']))
#			log.info(aatLog['netConnectivityList'])
			if type(aatLog['netConnectivityList']) == list:
				netlst = aatLog['netConnectivityList']
			else:
				netlst = json.loads(aatLog['netConnectivityList'])
		except Exception, e:
			netlst = aatLog['netConnectivityList'].strip('[ ]').split(',')

		for netItem in netlst:
			if netItem == None:
				break
			if netItem.find('|') < 0:
				if len(netItem) > 0:
					continue
				else:
					break
			netElm = netItem.strip(" u'\"").replace("||", "|").split('|')
			netDict = {}
			if netElm[2].find('WIFI') >= 0:
				if len(netElm) == 7:
					netDict['playSessionID'] = aatLog['playSessionId']
					netDict['stTime'] = netElm[0]
					netDict['ntype'] = 'w'
					netDict['bssid'] = netElm[4]
					netDict['ssid'] = netElm[3].replace("'", "''")
					netDict['traffic'] = netElm[6]
					rstNetList.append(netDict.copy())
			else:
				if len(netElm) == 5:
					netDict['playSessionID'] = aatLog['playSessionId']
					netDict['stTime'] = netElm[0]
					netDict['ntype'] = 'm'
					netDict['traffic'] = netElm[4]
					rstNetList.append(netDict.copy())


	except Exception, e:
		log.error("getNetconnInfo error:[%s]%s" % (type(e), e))
		log.error(aatLog)
		raise e

#Following function is not related to Open DD. You should ignore it
def insertNetInfo(waveCursor, netList):
	try:
		strLst = []
		for netElm in netList:
			if netElm['ntype'] == 'w':
				strValue = "('%s', %s, '%s', '%s', '%s', %s, unix_timestamp())"  % \
			           (netElm['playSessionID'], netElm['stTime'], netElm['ntype'], \
			            netElm['bssid'], netElm['ssid'], netElm['traffic'])
			else:
				strValue = "('%s', %s, '%s', NULL, NULL, %s, unix_timestamp())"  % \
				           (netElm['playSessionID'], netElm['stTime'], netElm['ntype'], netElm['traffic'])
			strLst.append(strValue)

		if len(strLst) > 0:
			sql = """insert into vidsession_net (playSessionID, stTime, ntype, bssid, ssid, traffic, lstuptmp)
					 values %s
 					 on duplicate key update ntype = values(ntype), bssid = values(bssid), ssid = values(ssid), traffic = values(traffic), lstuptmp = unix_timestamp()
				  """ % ', '.join(strLst)

			ret = waveCursor.execute(sql)
			if ret == 0:
				log.warn("insertNetInfo no record affected [%s]" % sql)

	except Exception, e:
		log.error("insertNetInfo error:%s" % e)
		log.error(strLst)
		raise e

def insertVidnet(waveCursor, vidDict):
	if vidDict == None:
		log.warn('vidnetDict is null')
		return False

	cols = vidDict.keys()
	vals = vidDict.values()

	try:
		slist = []
		for v in vals:
			if type(v) == str or type(v) == unicode:
				slist.append("'" + v.replace("'", "''") + "'")
			else:
				slist.append(str(v))

		sql = """insert into vidsession (%s, lstuptmp) values (%s, unix_timestamp())""" % (",".join(cols), unicode(",", "utf-8").join(slist))
		waveCursor.execute(sql)

	except Exception, e:
		log.error("INSERT VIDNET ERROR:%s" % e)
		log.error(vidDict)
		raise e


def insertVidnetLog(waveCursor, vidLogDict):
	if vidLogDict == None:
		log.warn('vidLogDict is null')
		return False

	cols = vidLogDict.keys()
	vals = vidLogDict.values()

	try:
		sql = """insert into vidsession_log (%s, lstuptmp) values (%s, unix_timestamp())""" % (",".join(cols), ",".join(["'" + str(val).replace("'", "''") + "'" for val in vals]))
		waveCursor.execute(sql)

	except Exception, e:
		log.error("INSERT vidsession_log ERROR:%s" % e)
		log.error(vidLogDict)
		raise e


def updateVidnet(waveCursor, vidDict):
	if vidDict == None:
		log.warn('updateVidnet : vidDict is null')
		return False

	playSessionID = vidDict.pop('playSessionID')
	cols = vidDict.keys()
	vals = vidDict.values()

	try:
		slist = []
		for key in vidDict:
			if type(vidDict[key]) == str or type(vidDict[key]) == unicode:
				s = "%s = '%s'" % (key, vidDict[key].replace("'", "''"))
			else:
				s = "%s = %s" % (key, str(vidDict[key]))
			slist.append(s)

		slist.append("lstuptmp = unix_timestamp()")

		#sql = "UPDATE vidsession SET %s WHERE playSessionID = '%s'" % (unicode(',', 'utf-8').join(map(lambda key:"%s='%s'" % (key, unicode(vidDict[key], 'utf-8').replace("'", "''")), vidDict)), playSessionID)
		sql = "UPDATE vidsession SET %s WHERE playSessionID = '%s'" % (unicode(',', 'utf-8').join(slist), playSessionID)
		waveCursor.execute(sql)

	except Exception, e:
		log.error("update vidsession ERROR:%s, playSessionID:%s" % (e, playSessionID))
		log.error(vidDict)
		raise e


###############################################################################################################
########################	   PROCESS By ONE AATLOG routine	   ############################################
###############################################################################################################
#
#Open DD processing only include following items in aatLog
#The others should be ignored.
#
# 'log_time', 'abrMode', 'agentAatOnOff',
# 'agentLogEndTime', 'agentLogStartTime', 'agentLogType', 'bbCount',
# 'bbList', 'brand', 'confOperator', 'deviceID', 'liveCurrentTSBitrate', 'model', 'netActiveNetwork', 'netCellState',
# 'netCID', 'netLAC', 'numTotalHits', 'osVer', 'pkgName', 'playAccBufferingTime',
# 'playAppPackageName', 'playContentId', 'playHost',
# 'playOrigin', 'playPlayingTime', 'playPreparingTime',
# 'playServiceMode', 'playSessionId', 'playTitle', 'requestBR', 'sID',
# 'trafficAgentMoAveBW', 'trafficAgentMoBytes', 'trafficAgentWFAveBW', 'trafficAgentWFBytes', 'trafficSystemMoRxBytes',
# 'trafficSystemWFRxBytes', 'playEndState', 'tTM', 'verCode', 'vID'
#
###############################################################################################################
class ProcessAATLog(object):
# for debug temp
	OW_TASK_SUBSCRIBE_EVENTS = ['evtPlayerLog']
#	OW_TASK_SUBSCRIBE_EVENTS = []
# for debug temp
	OW_TASK_PUBLISH_EVENTS = []
	OW_USE_HASHING = False
	OW_HASH_KEY = None
	OW_NUM_WORKER = 16

	def publishEvent(self, event, params):
		# THIS METHOD WILL BE OVERRIDE
		# DO NOT EDIT THIS METHOD
		pass

	def handler(self, aatLog):
		try:
			waveCursor = None

			#update apmain.mdev's plmnid for hoppin case
			if aatLog.get('confOperator', '') > '':
				updateMcc(aatLog.get('deviceID', ''), aatLog['confOperator'])

			#in case of Off Log, process only End(0) log
			if int(aatLog.get('agentAatOnOff', -1)) == 0:
				if aatLog.get('agentLogType', -1) <> 0:
					return

			curAnID = aatLog.get('deviceID').strip(" u'")
			curPkgname = aatLog.get('pkgName').strip(" u'")
			curPsID = aatLog.get('playSessionId', '').strip(" u'")
			curTTM = Decimal(aatLog.get('tTM', 0.0))
			curEndTm = int(aatLog.get('agentLogEndTime', 0))

			waveCursor = worker.dbmanager.allocDictCursor('myapwave')
			waveCursor.execute("START TRANSACTION")

			strSQL= None
			strSQL = """SELECT a.*, IFNULL(b.psid, '') AS bpsid, b.*, e.* FROM
         (SELECT m.*, MAX(n.tTM) AS maxTTM, MAX(IF(n.tTM = %.3f, 1, 0) ) AS bExist
              FROM vidsession m LEFT OUTER JOIN vidsession_log n ON  m.playSessionID = n.playSessionID
              WHERE m.playSessionID = '%s') a LEFT OUTER JOIN
         (SELECT playSessionID AS psid, MAX(tTM) AS lstTTM,
              SUBSTR(MAX(CONCAT(RPAD(tTM, 14, '0'), logType)), 15) AS mLogType,
              SUBSTR(MAX(CONCAT(RPAD(tTM, 14, '0'), logEndTime)), 15) AS mLogEndTime,
              SUBSTR(MAX(CONCAT(RPAD(tTM, 14, '0'), IFNULL(bufferState, '0'))), 15) AS mBufferState,
              SUM(playTime) AS mPlayTime, SUM(pauseTime) AS mPauseTime,
              SUM(elapsedTime) AS mElapsedTime, SUM(cellRxBytes) AS mCellBytes, SUM(wfRxBytes) AS mWFBytes,
              SUM(cellDuration) AS mCellDur, SUM(wfDuration) AS mWFDur
              FROM vidsession_log
              WHERE playSessionID = '%s' AND tTM < %.3f) b
         ON a.playSessionID = b.psid
         LEFT OUTER JOIN
         (SELECT playSessionID AS psid, MIN(tTM) AS nextTTM
              FROM vidsession_log
              WHERE playSessionID = '%s' AND tTM > %.3f ) e
         ON a.playSessionID = e.psid
         """ % (curTTM, curPsID, curPsID, curTTM, curPsID, curTTM)
			ret = waveCursor.execute(strSQL)
			if ret > 0:
				row = waveCursor.fetchone()
				if row['playSessionID'] <> None:
					if row['bExist'] == 1:
						return
					else:
						vidupdate(waveCursor, aatLog, row)
				else:
					vidcreate(waveCursor, aatLog)
			else: 		# Insert new playsession
				vidcreate(waveCursor, aatLog)

			# get BB, BW
			#Following code is not related to Open DD. You should ignore it.
			#### BEGIN - IGNORE
			logSubList = []
			getBBinfo(logSubList , aatLog)
			insertBBSQL(waveCursor, logSubList)

			logSubList = []
			getNetConn(logSubList, aatLog)
			insertNetInfo(waveCursor, logSubList)
			#### END - IGNORE

			waveCursor.execute("COMMIT")

		except Exception, e:
			log.error("processAATLOG : %s" % e)
			log.error(aatLog)
			if strSQL <> None:
				log.error(strSQL)
			if waveCursor <> None:
				waveCursor.execute("ROLLBACK")
			if str(e).find('Deadlock')> 0:
				log.error("processAAATLog raise e")
				raise e

		finally:
			if waveCursor <> None:
				worker.dbmanager.freeCursor(waveCursor)


