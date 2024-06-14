# coding=utf-8
# Copyright (c) 2024 Huawei Technologies Co., Ltd.
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import functools
import json
import threading
import time

from oslo_concurrency import lockutils
from oslo_log import log
from oslo_utils import strutils
import requests
from requests.adapters import HTTPAdapter

from ..utils import constants, driver_utils

LOG = log.getLogger(__name__)


class HostNameIgnoringAdapter(HTTPAdapter):
    def cert_verify(self, conn, url, verify, cert):
        conn.assert_hostname = False
        return super(HostNameIgnoringAdapter, self).cert_verify(
            conn, url, verify, cert)


def rest_operation_wrapper(func):
    @functools.wraps(func)
    def wrapped(self, url, **kwargs):
        full_url = self.base_url + url
        if not kwargs.get('log_filter'):
            LOG.info('URL: %(url)s Method: %(method)s Data: %(data)s',
                     {'url': full_url, 'method': func.__name__,
                      'data': kwargs.get('data')})

        if 'timeout' not in kwargs:
            kwargs['timeout'] = constants.SOCKET_TIMEOUT

        old_token = self._session.headers.get('X-Auth-Token')
        kwargs['old_token'] = old_token
        kwargs['full_url'] = full_url

        result = check_retry(self, func, kwargs)

        if not kwargs.get('log_filter'):
            LOG.info('Response: %s', result)
        return result

    return wrapped


def rest_set_semaphore(func):
    @functools.wraps(func)
    def wrapped(self, url, **kwargs):
        self.semaphore.acquire()
        result = func(self, url, **kwargs)
        self.semaphore.release()
        return result
    return wrapped


def check_retry(self, func, kwargs):
    """check weather need to retry.

    if don't need retry call func one time and return
    if need retry will retry until the function don't catch defined error
    or reach the max retry times, and the last retry time will not catch error.
    """
    for retry_time in range(constants.DME_REQUEST_RETRY_TIMES):
        retry_interval = driver_utils.get_retry_interval(retry_time + 1)
        result = do_retry(self, func, retry_interval, kwargs)
        if result:
            return result

    with self.call_lock.read_lock():
        res = func(self, kwargs.get('full_url'), **kwargs)
    duration_time = res.elapsed.total_seconds()
    result = res.json()
    if not isinstance(result, dict):
        result = {'data': result}
    _, error_code = self._error_code(res)
    result['duration'] = duration_time
    result['error_code'] = error_code
    return result


def do_retry(self, func, retry_interval, kwargs):
    full_url = kwargs.get('full_url')
    old_token = kwargs.get('old_token')
    try:
        with self.call_lock.read_lock():
            res = func(self, full_url, **kwargs)
    except (requests.Timeout, requests.ConnectionError) as err:
        LOG.warning("Failed to call the url, "
                    "trying to retry, the url is %s,"
                    "err info is %s" % (full_url, err))
        time.sleep(retry_interval)
        self.retry_relogin(old_token)
        return {}
    else:
        duration_time = res.elapsed.total_seconds()
        result = res.json()
        status_code, error_code = self._error_code(res)
        if not isinstance(result, dict):
            result = {'data': result}
        if any((error_code in constants.DME_RETRY_RELOGIN_CODE,
                status_code in constants.DME_RETRY_RELOGIN_CODE)):
            LOG.warning("the error code is abnormal, "
                        "trying to retry, the url is %s,"
                        "result info is %s" % (full_url, result))
            time.sleep(retry_interval)
            self.retry_relogin(old_token)
            return {}
        elif any((error_code in constants.DME_RETRY_CODE,
                  status_code in constants.DME_RETRY_CODE)):
            LOG.warning("the error code is abnormal, "
                        "trying to retry, the url is %s,"
                        "result info is %s" % (full_url, result))
            time.sleep(retry_interval)
            return {}
        else:
            result['duration'] = duration_time
            result['error_code'] = error_code
            return result


class RestClient(object):
    def __init__(self, driver_config):
        self.driver_config = driver_config
        self.semaphore = threading.Semaphore(self.driver_config.semaphore)
        self.call_lock = lockutils.ReaderWriterLock()
        self._session = None
        self._login_url = None

    @staticmethod
    def _error_code(res):
        raise NotImplementedError

    def retry_relogin(self, old_token):
        raise NotImplementedError

    def call(self, url=None, data=None, method=None, ex_url=None, log_filter=False):
        """Send requests to server.if fail, try another RestURL."""
        function_enum = {
            'GET': self.get,
            'PUT': self.put,
            'POST': self.post,
            'DELETE': self.delete,
        }
        func = function_enum.get(method)
        kwargs = {
            'timeout': constants.SOCKET_TIMEOUT,
            'ex_url': ex_url,
            'log_filter': log_filter
        }

        return func(url, data=data, **kwargs)

    @rest_operation_wrapper
    @rest_set_semaphore
    def get(self, url, **kwargs):
        if 'data' in kwargs:
            return self._session.get(
                url, data=json.dumps(kwargs.get('data')),
                timeout=kwargs.get('timeout'),
                verify=self._session.verify)
        else:
            return self._session.get(
                url, timeout=kwargs.get('timeout'),
                verify=self._session.verify)

    @rest_operation_wrapper
    @rest_set_semaphore
    def post(self, url, **kwargs):
        return self._session.post(
            url, data=json.dumps(kwargs.get('data')),
            timeout=kwargs.get('timeout'),
            verify=self._session.verify)

    @rest_operation_wrapper
    @rest_set_semaphore
    def put(self, url, **kwargs):
        return self._session.put(
            url, data=json.dumps(kwargs.get('data')),
            timeout=kwargs.get('timeout'),
            verify=self._session.verify)

    @rest_operation_wrapper
    @rest_set_semaphore
    def delete(self, url, **kwargs):
        if 'data' in kwargs:
            return self._session.delete(
                url, data=json.dumps(kwargs.get('data')),
                timeout=kwargs.get('timeout'),
                verify=self._session.verify)
        else:
            return self._session.delete(
                url, timeout=kwargs.get('timeout'),
                verify=self._session.verify)

    def init_http_head(self, data, url):
        self._session = requests.Session()
        self._session.headers.update({
            "Connection": "keep-alive",
            'Accept': 'application/json',
            "Content-Type": "application/json; charset=utf-8"})
        ssl_verify = strutils.bool_from_string(self.driver_config.ssl_verify, default=True)
        self._session.verify = False
        if ssl_verify:
            self._session.verify = self.driver_config.ssl_cert_path
        self._session.mount(self.driver_config.rest_url.lower(),
                            HostNameIgnoringAdapter())
