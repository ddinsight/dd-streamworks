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
Created on 2012. 7. 19.

@author: springchoi
"""

import sys
from datatype import *
import worker

import re
import traceback
import time
import datetime
import collections
import MySQLdb

try:
    from worker import log
except ImportError:
    import logging as log


Priority = collections.namedtuple('Priority', 'LOW NORMAL HIGH')._make(range(3))


class UniqueList(list):
    key = lambda x: x

    def setKey(self, key=lambda x: x):
        if not callable(key):
            raise RuntimeError("Key is not callable")
        self.key = key

    def addSet(self, item, key=None):
        if not key:
            key = self.key
        elif not callable(key):
            raise RuntimeError("Key is not callable")
        if len(filter(lambda x: key(x) == key(item), self)) > 0:
            return False
        self.append(item)
        return True


class GeoInfo(collections.namedtuple('_GeoInfo', 'lat, lng, acc, geosrc, from_cell')):
    def __new__(cls, lat=-9999, lng=-9999, acc=50000, geosrc='unknown', from_cell=False):
        # add default values
        return super(GeoInfo, cls).__new__(cls, lat, lng, acc, geosrc, from_cell)


class WiFiNode(collections.namedtuple('_WiFiNode', 'state, bssid, ssid, rssi, regdtm, bregap, bmap, optrcom, geoloc, priority')):
    __registerdApSSIDPattern = {'LGT':('U\+',), 'KT':('olleh_GiGA_WiFi', 'ollehWiFi', 'NESPOT', 'QOOKnSHOW'), 'SKT':('T wifi zone',)}
    __hotspotApSSIDPattern = ('AndroidHotspot', 'AndroidAP', 'HTC-', 'Galaxy ', 'SKY A')
    __mobileApSSIDPattern = ('WibroEgg', 'ollehEgg', 'KWI-B', 'SHOW_JAPAN_EGG', 'egg\Z')

    def __new__(cls, state, bssid, ssid='', regdtm='19000101000000', rssi=-200, bregap=False, bmap=False, optrcom='none', geoloc=None, priority=Priority.NORMAL):
        # Classify WiFi
        try:
            if ssid not in ('', None):
                ssid = re.sub(r'^\s*"(.*)"\s*$', r'\1', unicode(ssid))
                if ssid.find('"') >= 0:
                    log.error("!!! SSID - %s" % ssid)
                if cls.isHotspot(ssid):
                    priority = Priority.LOW
                else:
                    optrcom = cls.getWiFiOperator(ssid)
                    bregap = True if optrcom != 'none' else False
                    if not bregap:
                        bmap = cls.isMobile(ssid)

                try:
                    ssid = MySQLdb.escape_string(unicode(ssid).encode('utf-8'))
                except Exception, e:
                    # Non-ascii data.
                    log.warn("SSID MySQLdb.escape_string Error - %s, %s" % (ssid, e))

            if not geoloc:
                geoloc = GeoInfo()
        except Exception, e:
            log.error(e)
            log.error('BSSID - %s, SSID - %s' % (bssid, ssid))
            exc_type, exc_value, exc_traceback = sys.exc_info()
            log.error(traceback.format_exception(exc_type, exc_value, exc_traceback))
            raise e

        return super(WiFiNode, cls).__new__(cls, state, bssid, ssid, rssi, regdtm, bregap, bmap, optrcom, geoloc, priority)

    @classmethod
    def isHotspot(cls, ssid):
        patt = r'%s' % '|'.join(cls.__hotspotApSSIDPattern)
        if re.match(patt, ssid, re.IGNORECASE):
            #log.info("%s - Hotspot SSID, drop this AP" % ssid)
            return True

    @classmethod
    def getWiFiOperator(cls, ssid):
        for provider in cls.__registerdApSSIDPattern.keys():
            patt = r'%s' % '|'.join(cls.__registerdApSSIDPattern[provider])
            if re.match(patt, ssid, re.IGNORECASE):
                #log.info("Registered SSID - %s" % ssid)
                return provider
        return 'none'

    @classmethod
    def isMobile(cls, ssid):
        patt = r'%s' % '|'.join(cls.__mobileApSSIDPattern)
        if re.search(patt, ssid, re.IGNORECASE):
            #log.info("Mobile AP - %s" % ssid)
            return True
        return False


class CellNode(collections.namedtuple('_CellNode', 'state, cellid, plmnid, cid, lac, celltype, regdtm, geoloc, priority')):
    def __new__(cls, state, cellid, celltype=0, regdtm='19000101000000', geoloc=None, priority=Priority.NORMAL):
        # add default values
        try:
            plmnid, cid, lac = cellid.split('_')

            # guard from invalid data
            if len(plmnid) > 6 or int(plmnid) == 0:
                plmnid = '0'

            if not geoloc:
                geoloc = GeoInfo()
        except Exception, e:
            raise e
        return super(CellNode, cls).__new__(cls, state, cellid, plmnid, cid, lac, celltype, regdtm, geoloc, priority)


def addWiFi(cursor, node):
    strSql = """INSERT INTO
                        apmain.apinfo (bssid, ssid, regdtm, bregap, bmap, lat, lng, acc, geosrc, optrcom, seq)
                    VALUES('%s','%s','%s','%d','%d','%f','%f','%d','%s','%s','%s')
                    ON DUPLICATED UPDATE
                      lat = IF(VALUES(seq) > seq, VALUES(lat), lat),
                      lng = IF(VALUES(seq) > seq, VALUES(lng), lng),
                      seq = IF(VALUES(seq) > seq, VALUES(seq), seq),
                      acc = IF(VALUES(seq) > seq, VALUES(acc), acc),
                      geosrc=VALUES(geosrc)"""

    try:
        strSql = strSql % (node.bssid, node.ssid, node.regdtm, int(node.bregap), int(node.bmap), node.geoloc.lat, node.geoloc.lng, node.geoloc.acc, node.geoloc.geosrc, node.optrcom, node.rssi)
    except Exception, e:
        log.error("SQL GEN ERR - %s" % bytes(node.ssid))
        strSql = strSql % (node.bssid, '', node.regdtm, int(node.bregap), int(node.bmap), node.geoloc.lat, node.geoloc.lng, node.geoloc.acc, node.geoloc.geosrc, node.optrcom, node.rssi)

    try:
        cursor.execute(strSql)
        log.debug("INSERT - %s" % node.bssid)
    except Exception, e:
        # Duplicate entry error
        if e[0] != 1062:
            log.error(e)
            log.error(strSql)
        return False

    return True


netTypeCode = {'gsm':1, 'cdma':2, 'lte':3}


def addCellTower(cursor, node):
    strSql = """INSERT INTO
                    apmain.cellinfo (fullid, plmnid, cellid, lac, celltype, regdtm, lat, lng, acc, geosrc, seq)
                VALUES('%s','%s','%s','%s','%d','%s','%s','%f','%f','%s', '1')
                ON DUPLICATED UPDATE lat=((lat*seq)+VALUES(lat))/(seq+1), lng=((lng*seq)+VALUES(lng))/(seq+1), seq=seq+1, geosrc=VALUES(geosrc)"""

    try:
        strSql = strSql % (node.cellid, node.plmnid, node.cid, node.lac, 0, node.regdtm, node.geoloc.lat, node.geoloc.lng, node.geoloc.acc, 'cellLoc' if node.geoloc.from_cell else node.geoloc.geosrc)
        cursor.execute(strSql)
        log.debug("INSERT - %s" % node.cellid)
    except Exception, e:
        # Duplicate entry error
        if e[0] != 1062:
            log.error(e)
            log.error(strSql)
        return False
    return True


class ProcessNetworkNode(object):
    OW_TASK_SUBSCRIBE_EVENTS = ['evtPlayerLog', 'evtNetworkLog']
    OW_TASK_PUBLISH_EVENTS = []
    OW_USE_HASHING = False
    OW_HASH_KEY = None
    OW_NUM_WORKER = 8

    def publishEvent(self, event, params):
        # THIS METHOD WILL BE OVERRIDE
        # DO NOT EDIT THIS METHOD
        pass

    def __makeCellId(self, plmnid, cid, lac):
        try:
            cellId = map(lambda x: str(x) if str(x).isdigit() else '0', [plmnid, cid, lac])
            if 0 not in map(int, cellId) and len(cellId[0]) < 7:
                return '_'.join(cellId)
        except Exception, e:
            log.error(e)

        return None

    def extractNetworkNode(self, params):
        # net info structure
        # wifi : 'wifi', status, ssid, bssid
        # cell : 'cell', status, celltower id
        # status : 'active' for current network, 'inactive' for logged network

        timestamp = time.strftime('%Y%m%d%H%M%S', time.gmtime(params['tTM']))
        logType = params.get('log_type', 'unknown')
        netList = UniqueList()
        netList.setKey(key=lambda x: x.cellid if isinstance(x, CellNode) else x.bssid)

        if 'lat' in params and 'lng' in params:
            geoloc = GeoInfo(lat=params.get('lat'), lng=params.get('lng'), acc=params.get('accuracy', 500), geosrc='device')
        else:
            geoloc = None

        # APAT Header fields
        try:
            if 'pwf' in params:
                pwf = params['pwf']
                if 'bssid' in pwf and EthAddrType.isEthAddr(pwf['bssid']):
                    node = WiFiNode(state='active', bssid=pwf['bssid'], ssid=pwf.get('ssid', ''), regdtm=timestamp, geoloc=geoloc)
                    netList.addSet(node)
        except Exception, e:
            log.error(e)
            log.error(params)

        try:
            if 'pcell_list' in params and isinstance(params['pcell_list'], list) and len(params['pcell_list']) > 0:
                pcell = params['pcell_list'][0]
                if 'cid' in pcell and 'lac' in pcell:
                    cellId = self.__makeCellId(int("%03d%02d" % (pcell.get('mcc', 0), pcell.get('mnc', 0))), pcell.get('cid'), pcell.get('lac'))
                    if cellId:
                        if 'ctype' in pcell and str(pcell['ctype']).isdigit():
                            ctype = int(pcell.get('ctype', -1)) + 1     # -1 : Unknown
                            cellType = ctype if ctype in netTypeCode.values() else 0
                        else:
                            cellType = 0
                        node = CellNode(state='active', cellid=cellId, celltype=cellType, regdtm=timestamp, geoloc=geoloc, priority=Priority.HIGH)
                        netList.addSet(node)
        except Exception, e:
            log.error(e)
            log.error(params)

        return netList

    def handler(self, params):
        # Event Key/Value 인 경우, 무시
        if 'evtKey' in params.keys():
            return

        # Extract network nodes from logging
        try:
            networks = self.extractNetworkNode(params)
        except Exception, e:
            log.error(e)
            log.error(params)
            return

        cursor = worker.dbmanager.allocDictCursor('myapmain')

        try:
            for node in networks:
                nodeType = 'wifi' if isinstance(node, WiFiNode) else 'cell'
                if nodeType == 'wifi' and node.priority > Priority.LOW:
                    try:
                        addWiFi(cursor, node)
                    except Exception, e:
                        log.error(e)
                elif nodeType == 'cell':
                    try:
                        addCellTower(cursor, node)
                    except Exception, e:
                        log.error(e)
        except Exception, e:
            log.error(e)
            exc_type, exc_value, exc_traceback = sys.exc_info()
            log.error(traceback.format_exception(exc_type, exc_value, exc_traceback))

        worker.dbmanager.freeCursor(cursor)


class CheckCellPCIField(object):
    OW_TASK_SUBSCRIBE_EVENTS = ['evtPlayerLog', 'evtNetworkLog']
    OW_TASK_PUBLISH_EVENTS = []
    OW_USE_HASHING = False
    OW_HASH_KEY = None
    OW_NUM_WORKER = 8

    def __init__(self):
        pass

    def publishEvent(self, event, params):
        # THIS METHOD WILL BE OVERRIDE
        # DO NOT EDIT THIS METHOD
        pass

    def handler(self, params):
        if not isinstance(params, dict):
            return

        pcell = params.get('pcell')
        pcellList = params.get('pcell_list')

        if not pcell or not pcellList:
            return

        try:
            plmnid = pcell.get('mcc') + pcell.get('mnc')
        except Exception, e:
            return

        pciInfo = {}

        for cell in pcellList:
            if 'pci' not in cell:
                continue

            try:
                cellId = '%s_%s_%s' % (plmnid, cell['cid'], cell['tac'])
            except Exception, e:
                continue

            pciInfo[cellId] = cell['pci']

        cursor = worker.dbmanager.allocDictCursor('myapmain')
        strSql = "UPDATE apmain.cellinfo SET pci='%s' WHERE fullid=%s and pci is null"

        try:
            cursor.executemany(strSql, [(v[1], v[0]) for v in pciInfo.items()])
        except Exception, e:
            log.error(e)

        worker.dbmanager.freeCursor(cursor)
