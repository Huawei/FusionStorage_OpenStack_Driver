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
VOLUME_NOT_EXIST = (31000000, 50150005, 32150005)
SNAPSHOT_NOT_EXIST = (50150006,)

BASIC_URI = '/dsware/service/'
CONF_PATH = "/etc/cinder/cinder.conf"
HOST_GROUP_PREFIX = "OpenStack_"

CONF_ADDRESS = "dsware_rest_url"
CONF_MANAGER_IP = "manager_ips"
CONF_POOLS = "dsware_storage_pools"
CONF_PWD = "san_password"
CONF_USER = "san_login"
CONF_IP = "san_ip"
CONF_PORT = "san_port"
CONF_NEW_POOLS = "storage_pools"
CONF_STORAGE_CA_FILEPATH = "storage_ca_filepath"
CONF_STORAGE_KEY_FILEPATH = "storage_key_filepath"
CONF_STORAGE_CERT_FILEPATH = "storage_cert_filepath"
CONF_STORAGE_SSL_TWO_WAY_AUTH = "storage_ssl_two_way_auth"

DEFAULT_WAIT_INTERVAL = 5
MIGRATION_COMPLETE = 76
MIGRATION_FAULT = 74
STATUS_HEALTH = 1
STATUS_VOLUME_READY = 27
MIGRATION_WAIT_INTERVAL = 5
DEFAULT_WAIT_TIMEOUT = 3600 * 24 * 30

QOS_MUST_SET = ["maxIOPS", "maxMBPS"]
QOS_KEYS = ["maxIOPS", "maxMBPS", "total_iops_sec", "total_bytes_sec"]
QOS_SCHEDULER_KEYS = [
    "scheduleType", "startDate", "startTime",
    "durationTime", "dayOfWeek"
]
QOS_PREFIX = "OpenStack_"
QOS_SCHEDULER_DEFAULT_TYPE = "0"
QOS_SCHEDULER_WEEK_TYPE = "3"
QOS_SUPPORT_SCHEDULE_VERSION = "8.0"
QOS_MAX_INTERCEPT_LENGTH = 36
SECONDS_OF_DAY = 24 * 60 * 60
SECONDS_OF_HOUR = 60 * 60
SNAPSHOT_HEALTH_STATUS = (
    SNAPSHOT_HEALTH_STATS_NORMAL,
    SNAPSHOT_HEALTH_STATS_FAULT) = (1, 2)
SNAPSHOT_RUNNING_STATUS = (
    SNAPSHOT_RUNNING_STATUS_ONLINE,
    SNAPSHOT_RUNNING_STATUS_OFFLINE,
    SNAPSHOT_RUNNING_STATUS_ROLLBACKING) = (27, 28, 44)
SNAPSHOT_ROLLBACK_PROGRESS_FINISH = 100
SNAPSHOT_ROLLBACK_TIMEOUT = 60 * 60 * 24
WAIT_INTERVAL = 10
WEEK_DAYS = ["Sun", "Mon", "Tue", "Wed", "Thur", "Fri", "Sat"]
TIMEZONE = {"Asia/Beijing": "Asia/Shanghai"}
MAX_NAME_LENGTH = 31
MAX_IOPS_VALUE = 999999999
MAX_MBPS_VALUE = 999999
HOST_FLAG = 0
URL_NOT_FOUND = "Not Found for url"
HOST_ISCSI_RELATION_EXIST = 540157748
DSWARE_MULTI_ERROR = 1
HOST_ALREADY_EXIST = 50157019
HOST_MAPPING_EXIST = 50157027
HOST_MAPPING_GROUP_EXIST = 50157046
HOST_ALREADY_MAPPING_LUN = 50157058
HOSTGROUP_ALREADY_EXIST = 50157044
INITIATOR_ALREADY_EXIST = 50155102
INITIATOR_IN_HOST = 50157021

CHECK_CLONED_INTERVAL = 2
REST_VOLUME_CREATING_STATUS = 15
REST_VOLUME_DUPLICATE_VOLUME = 6
REST_VOLUME_CREATE_SUCCESS_STATUS = 0
CLONE_VOLUME_TIMEOUT = 3600 * 24 * 30

QOS = 'qos'
