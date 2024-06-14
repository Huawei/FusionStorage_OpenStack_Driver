# coding=utf-8
# Copyright (c) 2021 Huawei Technologies Co., Ltd.
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

import ssl
import time
from oslo_log import log
from oslo_serialization import jsonutils
from oslo_utils import strutils
from six.moves import http_cookiejar
from six.moves.urllib import request as urlreq

from manila import exception
from manila.i18n import _

from ..utils import constants

LOG = log.getLogger(__name__)


class SendRequest:
    def __init__(self, driver_config):
        self.driver_config = driver_config
        self.login_info = {}
        self.headers = {}
        self.url = None
        self.cookie = None

    @staticmethod
    def _get_error_code(ex_url, result):
        error_code = None
        result_obj = result['result']
        if ex_url:
            error_code = result_obj
            if error_code == 2:
                error_code = int(result['errorCode'])
        elif 'error' in result.keys():
            error_code = result['error']['code']
        elif 'result' in result.keys():
            if isinstance(result_obj, int):
                error_code = result_obj
            elif isinstance(result_obj, dict):
                error_code = result_obj['code']

        return int(error_code)

    def call(self, url=None, data=None, method=None, ex_url=None, log_call=True):
        """Send requests to server.if fail, try another RestURL."""

        result = None
        for num in range(6):
            if 0 < num < 5:
                msg = _("Try again after 10 seconds.")
                LOG.error(msg)
                time.sleep(10)
            elif num == 5:
                msg = _("Call fail for TIME OUT.")
                LOG.error(msg)
                break

            if not (url or ex_url):
                try:
                    self._login()
                    break
                except Exception:
                    msg = (_("login fail, try again."))
                    LOG.warning(msg)
                    continue

            result = self._do_call(url, data, method, ex_url, log_call)
            if self._check_error_code(ex_url, result):
                break
            else:
                continue

        return result

    def _login(self):
        """Log in huawei oceanstorPacific array."""

        self._get_login_info()
        self.cookie = http_cookiejar.CookieJar()
        self.headers = {
            "Connection": "keep-alive",
            "Content-Type": "application/json",
        }
        self.url = self.login_info.get('RestURL')

        url = "aa/sessions"
        data = jsonutils.dumps(
            {"username": self.login_info.get('UserName'),
             "password": self.login_info.get('UserPassword'),
             "scope": "0"})
        result = self._do_call(url, data, log_call=False)

        if not result or (result.get('result', {}).get('code') != 0) or ("data" not in result):
            err_msg = ("Login to {0} failed. Result: {1}.".format(self.url + url, result))
            raise exception.InvalidHost(reason=err_msg)

        self.headers['x-auth-token'] = result.get('data', {}).get('x_auth_token')
        LOG.info("login success for url:{0}.\n".format(self.url))

    def _do_call(self, url, data=None, method=None, ex_url=None, log_call=True):

        if url:
            url = self.url + url
        if ex_url:
            url = self.url.replace('/api/v2/', ex_url)

        self._init_http_opener()
        try:
            if isinstance(data, str):
                data = data.encode()
            req = urlreq.Request(url, data, self.headers)

            def get_method():
                return method

            if method:
                req.get_method = get_method
            res_temp = urlreq.urlopen(req, timeout=constants.SOCKET_TIMEOUT)
            res = res_temp.read().decode("utf-8")
            result = jsonutils.loads(res, encoding='utf-8')
            if log_call:
                LOG.info("The url is: {url}, "
                         "The method is {method}, "
                         "Request Data is {data}, "
                         "Response is {res}".format(url=url, method=method, data=data, res=result))
        except Exception as e:
            LOG.error(_("Bad response from server: {0}. Error: {1}".format(url, e)))
            result = {"result": {"code": -1, "description": "Connect server error"}}

        return result

    def _get_login_info(self):
        """Get login IP, username and password from config file."""

        login_info = {}
        login_info['RestURL'] = self.driver_config.rest_url
        login_info['UserName'] = self.driver_config.user_name
        pwd = self.driver_config.user_password
        login_info['UserPassword'] = pwd

        ssl_verify = self.driver_config.ssl_verify
        login_info['SslCertVerify'] = strutils.bool_from_string(ssl_verify, default=True)
        ssl_path = self.driver_config.ssl_cert_path
        login_info['SslCertPath'] = ssl_path

        self.login_info = login_info

    def _init_http_opener(self):
        """
        if ssl module miss function create_default_context and
        _create_stdlib_context,then raise attributeError exception.
        """
        try:
            handlers = (urlreq.HTTPCookieProcessor(self.cookie),)
            ssl_context = ssl._create_unverified_context()
            ssl._create_default_https_context = ssl._create_unverified_context
            handlers = handlers + (urlreq.HTTPSHandler(context=ssl_context),)
            opener = urlreq.build_opener(*handlers)
        except AttributeError:
            LOG.debug('ssl module miss function create_default_context '
                      'or _create_stdlib_context')
            handlers = urlreq.HTTPCookieProcessor(self.cookie)
            opener = urlreq.build_opener(handlers)
        urlreq.install_opener(opener)

    def _check_error_code(self, ex_url, result):

        error_code = self._get_error_code(ex_url, result)
        if error_code == constants.ERROR_URL_OPEN or error_code in constants.ERROR_SPECIAL_STATUS:
            msg = (_("The server is currently abnormal or busy."))
            LOG.warning(msg)
            return False
        elif error_code == constants.ERROR_USER_OFFLINE or error_code == constants.ERROR_NO_PERMISSION:
            msg = (_("The token has expired. Re-login."))
            LOG.warning(msg)
            try:
                self._login()
            except Exception:
                msg = (_("Re-login fail."))
                LOG.warning(msg)
            return False
        else:
            return True
