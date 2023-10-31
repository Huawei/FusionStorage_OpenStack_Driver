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

SOCKET_TIMEOUT = 60
SSL_AUTHENTICATION_TIMEOUT = 600

# error code of RE-LOGIN
ERROR_USER_OFFLINE = 1077949069
ERROR_NO_PERMISSION = 1077949058

# error code of TRY AGAIN
ERROR_URL_OPEN = -1
ERROR_SPECIAL_STATUS = (
    33564721,
    33564722,  # tier busy
    33564699,  # filesystem busy
    33656845,  # namespace busy
    37000212,  # internal error
    33561653,
    37120053,  # remote replication busy
    33759517,
    33759518,  # KMS busy
    37000213,  # FSM busy
    37100145,  # FSM remote busy
    50092080,
    33609729,  # reclaiming junk data busy
    31000923,  # delete pool busy
    30400010,  # switch the active node busy
    30160010,  # EDS faulty
    33605891,  # FSA MDC busy
    1073793460,  # system busy
    1077948995,  # memory busy
    1077949006,  # system busy
    1073793332,
    1073793333,  # set busy
    1077949001,  # message busy
    1077949004,  # process busy
    50400004,
    1077949021,  # upgrading
    33623351,  # object name is not exist
    33564736,  # file system id is not exist
    33564718  # specified path is not exist
)

POOL_STATUS_OK = (
    0,  # Normal
    5,  # Migrating data
    7,  # Degraded
    8  # Rebuilding data
)

# error code of NOT EXIST
ACCOUNT_NOT_EXIST = 1800000404
ACCOUNT_ALREADY_EXIST = 1800000409
UNIX_USER_NOT_EXIST = 37749540
UNIX_GROUP_NOT_EXIST = 37749520
NAMESPACE_NOT_EXIST = 33564678
QUOTA_NOT_EXIST = 37767685
QOS_NOT_EXIST = 33623307
TIER_NOT_EXIST = 33564719
POOL_NOT_EXIST = 50120003
NFS_SHARE_NOT_EXIST = 1077939726
CIFS_SHARE_NOT_EXIST = 1077939717
NFS_ACCOUNT_NOT_EXIST = 42514844
REPLICA_PAIR_NOT_EXIST = 37120003
NFS_SHARE_CLIENT_NOT_EXIST = 1077939728

# error code of ALREADY EXIST
NAMESPACE_ALREADY_EXIST = 33656844
QUOTA_ALREADY_EXIST = 37767684
QOS_ALREADY_EXIST = 33623308
TIER_ALREADY_EXIST = 33564716
QOS_ASSOCIATION_ALREADY_EXIST = 33623352
NFS_SHARE_EXIST = 1077939724
NFS_SHARE_CLIENT_EXIST = 1077939727
CIFS_SHARE_CLIENT_EXIST = 1077939718

# namespace config
SUPPORT_DPC = 0
NOT_SUPPORT_DPC = 1
NAMESPACE_READ_AND_WRITE = 0

# quota config
QUOTA_PARENT_TYPE_NAMESPACE = 40
QUOTA_TYPE_DIRECTORY = 1
QUOTA_UNIT_TYPE_BYTES = 0
QUOTA_UNIT_TYPE_GB = 3
QUOTA_TARGET_NAMESPACE = 1

# qos policy config
QOS_SCALE_NAMESPACE = 0
QOS_MODE_PACKAGE = 2
QOS_PACKAGE_SIZE = 10
MAX_BAND_WIDTH = 30720
BASIC_BAND_WIDTH = 2048
BPS_DENSITY = 250
MAX_IOPS = 3000000
BAND_WIDTH_UPPER_LIMIT = 1073741824
MAX_BPS_DENSITY = 1024000
MAX_IOPS_UPPER_LIMIT = 1073741824000

# Tier migration policy config
TIER_GRADE_HOT = '0'
TIER_GRADE_WARM = '1'
TIER_GRADE_COLD = '2'
PERIODIC_MIGRATION_POLICY = 0
MATCH_RULE_GT = 3
MATCH_RULE_LT = 2
DTIME_UNIT = 'day'
HTIME_UNIT = 'hour'
MTIME_DEFAULT = 7
MTIME_MAX = 1096
LEN_SQUASH = 2
DICT_ALL_SQUASH = {"all_squash": "0", "no_all_squash": "1"}
DICT_ROOT_SQUASH = {"root_squash": "0", "no_root_squash": "1"}
ATIME_UPDATE_CLOSE = 4294967295

# Pagination query config
MAX_QUERY_COUNT = 100
DSWARE_SINGLE_ERROR = 2
BYTE_TO_MB = 1024 * 1024
QOS_MODE_MANUAL = 3
