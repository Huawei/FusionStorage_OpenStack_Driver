# Copyright (c) 2016 Huawei Technologies Co., Ltd.
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

DEFAULT_TIMEOUT = 50
LOGIN_SOCKET_TIMEOUT = 32
GET_VOLUME_PAGE_NUM = 1
GET_VOLUME_PAGE_SIZE = 1000
GET_SNAPSHOT_PAGE_NUM = 1
GET_SNAPSHOT_PAGE_SIZE = 1000
GET_QOS_PAGE_NUM = 1
GET_QOS_PAGE_SIZE = 100

CONNECT_ERROR = 403
ERROR_UNAUTHORIZED = 10000003
ERROR_USER_OFFLINE = '1077949069'
VOLUME_NOT_EXIST = (31000000, 50150005)

BASIC_URI = '/dsware/service/'
CONF_PATH = "/etc/cinder/cinder.conf"
HOST_GROUP_PREFIX = "OpenStack_"

CONF_ADDRESS = "dsware_rest_url"
CONF_MANAGER_IP = "manager_ips"
CONF_POOLS = "dsware_storage_pools"
CONF_PWD = "san_password"
CONF_USER = "san_login"

QOS_MUST_SET = ["maxIOPS", "maxMBPS"]
QOS_KEYS = ["maxIOPS", "maxMBPS", "minBaselineIOPS", "minBaselineMBPS"]
QOS_SCHEDULER_KEYS = ["scheduleType", "startDate", "startTime",
                      "durationTime", "dayOfWeek"]
QOS_PREFIX = "OpenStack_"
QOS_SCHEDULER_DEFAULT_TYPE = "0"
QOS_SCHEDULER_WEEK_TYPE = "3"
QOS_SUPPORT_SCHEDULE_VERSION = "8.0"
SECONDS_OF_DAY = 24 * 60 * 60
SECONDS_OF_HOUR = 60 * 60
WEEK_DAYS = ["Sun", "Mon", "Tue", "Wed", "Thur", "Fri", "Sat"]
TIMEZONE = {"Asia/Beijing": "Asia/Shanghai"}
