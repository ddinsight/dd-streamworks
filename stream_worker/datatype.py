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


import datetime
import re


try:
    import log
except ImportError:
    import logging as log


class Type(object):
    __changed__ = False


class IntegerType(Type):
    def __init__(self, val = None):
        if val:
            return self.__call__(val)

    def __call__(self, val):
        print "INTEGER", val
        return int(val)


class StringType(Type):
    def __init__(self, val = None, lower = False, upper = False):
        self.lower = lower
        self.upper = upper
        if val:
            return self.__call__(val)

    def __call__(self, val):
        try:
            newVal = str(val)
        except Exception, e:
            log.error("Convert Error")
            raise e

        if self.lower:
            return str(newVal).lower()
        elif self.upper:
            return str(newVal).upper()
        else:
            return newVal


class EthAddrType(Type):

    def __init__(self, val = None, format='string'):
        self.format = format
        if val:
            return self.__call__(val)

    def __call__(self, val, format='string'):
        if self.isEthAddr(val):
            if self.format == 'string':
                return val.lower()
            elif self.format == 'number':
                return int(val.replace(':',''), 16)

        raise ValueError("Not EthAddrType : '%s'" % val)

    @staticmethod
    def isEthAddr(val):
        _pattHwAddr = '^([a-fA-F0-9]{2}:){5}[a-fA-F0-9]{2}$'
        try:
            if re.match(_pattHwAddr, val):
                return True
            else:
                return False
        except Exception, e:
            return False


def ethAddrToLong(val):
    _pattHwAddr = '^([a-fA-F0-9]{2}:){5}[a-fA-F0-9]{2}$'
    if re.match(_pattHwAddr, val):
        return int(val.replace(':',''), 16)
    return 0


def longToEthAddr(val):
        if not isinstance(val, (long, int)):
            raise ValueError("Cannot convert val to eth addr : " + val)

        tmp = hex(val).strip('0x*L').zfill(12)
        ethAddr = ':'.join(re.compile(r'.{2}').findall(tmp))
        return ethAddr


def checkIPAddress(val):
    try:
        tmpVal = (256, 255, 255, 255)
        nums = map(int, val.split('.'))

        #for IPv4
        if nums[0] < 0:
            for i in range(0, 4):
                nums[i] = str(tmpVal[i] + nums[i])
        val = ".".join(nums)
    except Exception, e:
        return val

    return val


def isPhoneNumber(val):
    if not isinstance(val, str):
        val = str(val)

    _pattPhone = '^[\+]([0-9]{12})$|^([0-9]{11})$|^([0-9]{10})$|^([0-9]{12})$'
    if re.match(_pattPhone, val):
        return True

    log.debug("Is not Phone number : %s" % val)
    return False


def checkPhoneNumber(val, country='82'):
    _pattPhone = '^[\+]([0-9]{12})$|^([0-9]{11})$|^([0-9]{10})$$'
    if re.match(_pattPhone, val):
        if len(val) == 13:
            return val.lstrip('+')
        elif len(val) == 11:
            return "%d%d" % (int(country), int(val))

    return None


class EthAddr(object):
    def __init__(self, bssid):
        _pattHwAddr = '^([a-fA-F0-9]{2}:){5}[a-fA-F0-9]{2}$'
        if not re.match(_pattHwAddr, bssid):
            raise ValueError("invalid ethernet address")

        self.bssid = bssid.replace(':','')

    def toNumber(self):
        return int(self.bssid, 16)

    def toHexString(self, width=12):
        return self.bssid

    def toHex(self):
        return self.bssid.lstrip('0')


def strToBool(val):
    '''
    Convert boolean string to boolean value
    '''
    if isinstance(val, bool):
        return val

    if str(val).lower() in ('true', 'active', 'on'):
        return True
    else:
        return False


def strToDatetime(val):
    '''
    convert date string to datetime.datetime value
    '''
    #log.debug("strToDatetime")
    if isinstance(val, datetime.datetime):
        return val

    if re.match("^[\d]{4}-(\d+)-(\d+) ((\d+):){2}(\d+)", val):
        return datetime.datetime.strptime(str(val).strip(), "%Y-%m-%d %H:%M:%S")
    else:
        try:
            timestamp = int(val)
        except Exception, e:
            raise (e[0], "strToDatetime : " + e[1])

        return datetime.datetime.fromtimestamp(timestamp)


def strToIntList(val, seperator):
    '''
    convert string of integer number list to integer list
    '''
    #log.debug("strToIntList : %s(%s)" % (str(val),str(type(val))))
    if isinstance(val, list):
        try:
            valList = map(int, val)
            return list(valList)
        except Exception, e:
            log.error("strToIntList Err : list contains non-integer value")
            return []

    if val == '':
        return []

    valList = str(val).split(seperator)

    try:
        valList = map(int, valList)
    except Exception, e:
        log.error("strToIntList : %s" % val)
        return []

    return valList


def strToDatetimeList(val, seperator):
    '''
    convert string of integer number list to integer list
    '''
    if isinstance(val, list):
        try:
            valList = map(strToDatetime, val)
            return list(valList)
        except Exception, e:
            return []

    if val == '':
        return []

    valList = str(val).split(seperator)

    try:
        valList = map(strToDatetime, valList)
    except Exception, e:
        log.error("strToDatetimeList : %s, %s" % (val, valList))
        return []

    return valList


def strToStrList(val, seperator):
    if isinstance(val, list):
        try:
            valList = map(str, val)
            return list(valList)
        except Exception, e:
            log.error("strToStrList : %s" % val)
            return []

    if val == '':
        return []

    valList = str(val).split(seperator)
    return map(str.strip, valList)


def getOperatorFromPlmnId(plmnId):
    try:
        id = int(plmnId)
        ret = divmod(id, 100)
        if ret[1] == 5:
            return "SKT"
        elif ret[1] == 8:
            return "KT"
        elif ret[1] == 6:
            return "LGT"

        # TODO : update to support global carrior


    except Exception, e:
        return None

