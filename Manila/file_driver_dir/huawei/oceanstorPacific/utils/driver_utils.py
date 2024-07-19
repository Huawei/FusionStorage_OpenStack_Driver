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

import random
import threading
import time

from oslo_log import log
from oslo_service import loopingcall

from manila import exception
from manila.i18n import _

from . import constants

LOG = log.getLogger(__name__)


def capacity_unit_up_conversion(raw_capacity, base_value, power):
    """
    Converting Capacity Units from Small to Large
    :param raw_capacity: Capacity to be converted
    :param base_value: Base of conversion unit
    :param power: power of base
    :return: Capacity After Conversion
    """
    return raw_capacity * (base_value ** power)


def capacity_unit_down_conversion(raw_capacity, base_value, power):
    """
    Converting Capacity Units from Large to Smller
    :param raw_capacity: Capacity to be converted
    :param base_value: Base of conversion unit
    :param power: power of base
    :return: Capacity After Conversion
    """
    return raw_capacity / (base_value ** power)


def convert_value_to_key(convert_enum, convert_value):
    """
    Convert enum value to key
    :param convert_enum: Enum to be converted
    :param convert_value: value to be converted
    :return: enum key
    """
    for key, value in convert_enum.items():
        if convert_value == value:
            return key
    return None


def get_retry_interval(retry_times):
    """
    Use the truncated binary exponential backoff
    algorithm to obtain the retry interval.
    :param retry_times: Number of Retries
    :return: the number of retry interval
    """
    if retry_times == 0:
        return 0
    return random.choice(range(0, 2 ** retry_times))


def wait_for_condition(func, interval, timeout):
    start_time = time.time()

    def _inner():
        result = func()

        if result:
            raise loopingcall.LoopingCallDone()

        if int(time.time()) - start_time > timeout:
            msg = (_('wait_for_condition: %s timed out.') % func.__name__)
            LOG.error(msg)
            raise exception.InvalidShare(reason=msg)

    timer = loopingcall.FixedIntervalLoopingCall(_inner)
    timer.start(interval=interval).wait()


def convert_capacity(cap, org_unit, tgt_unit):
    unit_list = [
        constants.CAP_BYTE,
        constants.CAP_KB,
        constants.CAP_MB,
        constants.CAP_GB,
        constants.CAP_TB,
    ]

    try:
        org_index = unit_list.index(org_unit.upper())
        tgt_index = unit_list.index(tgt_unit.upper())
    except ValueError as e:
        msg = _('unrecognized unit, org_unit: {0}, tgt_unit: {1}, error: {2}'.format(org_unit, tgt_unit, e))
        raise exception.InvalidInput(reason=msg)

    offset = tgt_index - org_index

    if offset > 0:
        return cap / (1024 ** offset)
    elif offset < 0:
        return cap * (1024 ** -offset)
    else:
        return cap


def add_or_update_dict_key(tgt_dict, tgt_key, tgt_value):
    """
    when tgt dict not have tgt_key，and tgt_key and set tgt_value in a  new list
    otherwise, append new_value in existed list
    """
    if tgt_key not in tgt_dict:
        tgt_dict[tgt_key] = [tgt_value]
    else:
        tgt_dict.get(tgt_key).append(tgt_value)


class MyThread(threading.Thread):
    def __init__(self, func, *args):
        super(MyThread, self).__init__()
        self.func = func
        self.args = args
        self.result_value = None

    def run(self):
        try:
            self.result_value = self.func(*self.args)
        except Exception as err:
            LOG.error("running threading function failed, err is %s" % err)
            self.result_value = {}

    def get_result(self):
        threading.Thread.join(self)
        return self.result_value
