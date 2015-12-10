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


import httplib
from urlparse import urlparse
import json

HTTPRESP_TIMEOUT = 408
__connection = {'http':httplib.HTTPConnection, 'https':httplib.HTTPSConnection}


class HTTPResponse(object):
    """
    HTTP Response 정보를 담는 Class
    HTTPResponse.code : HTTP 응답코드
    HTTPResponse.data : 데이터
    """
    def __init__(self, code, data = None):
        self.code = code
        self.data = data

        if data:
            try:
                self.data = json.loads(data)
            except ValueError:
                self.data = data
            except Exception, e:
                print e, data


def httpOperation(method, url, params={}, timeout=None):
    method = str(method).upper()

    if method not in ('GET', 'HEAD', 'POST', 'PUT', 'DELETE'):
        print "Invalid Method"
        return None

    urlData = urlparse(url)
    respData = None
    postData = None
    headers = {}
    connArgs = {}


    path = urlData.path

    if len(urlData.query) > 0:
        path += ('?' + urlData.query)

    if len(params) > 0:
        postData = json.dumps(params)
        headers = {"Content-type": "application/json"}

    func = __connection.get(urlData.scheme.lower(), httplib.HTTPConnection)

    connArgs['host'] = urlData.hostname
    connArgs['port'] = int(urlData.port) if urlData.port else None
    if timeout:
        connArgs['timeout'] = timeout

    conn = func(**connArgs)
    try:
        conn.request(method, path, postData, headers)
        response = conn.getresponse()
        #header = response.getheader('Content-Type')
        respCode = response.status
        respData = response.read()
    except httplib.socket.timeout, e:
        respCode = HTTPRESP_TIMEOUT
    except Exception, e:
        raise e
    finally:
        conn.close()

    return HTTPResponse(respCode, respData)
