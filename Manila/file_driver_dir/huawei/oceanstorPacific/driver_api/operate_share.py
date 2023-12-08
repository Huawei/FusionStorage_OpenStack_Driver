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

import math
from oslo_log import log
from oslo_utils import strutils

from manila import exception
from manila.i18n import _
from manila.share import api
from manila.share import share_types

from ..helper import constants

LOG = log.getLogger(__name__)
share_api = api.API()


class OperateShare(object):
    def __init__(self, helper, share):
        self.helper = helper
        self.share = share

        self.root = None
        self.free_pool = None
        self.domain = None  # 集群域名
        self.account_id = None  # 账户Id
        self.namespace_name = None  # 命名空间名称
        self.namespace_id = None  # 命名空间Id
        self.export_locations = None  # share路径信息
        self.quota_id = None  # 配额ID
        self.share_proto = self.share.get('share_proto', '').split('&')  # 共享协议类型
        self.tier_info = {}  # 分级策略信息
        self.qos_config = {}  # QOS策略信息

    def create_share(self, root, free_pool):

        self.root = root
        self.free_pool = free_pool

        self._check_domain()
        self._get_tier_info()
        self._get_qos_config()
        self._get_or_create_account()
        self._create_namespace()
        self._create_quote()
        self._create_qos()
        self._create_tier_migrate_policy()
        self._create_share_protocol()
        return self._get_location()

    def delete_share(self):
        if not self.share.get('export_locations') or not self.share.get('export_locations')[0].get('path'):
            LOG.warn(_(" share fail for invalid export location."))
            return False

        self._get_account_id()
        self._get_namespace_info()
        self._delete_share_protocol()
        self.helper.delete_qos(self.namespace_name)
        self.helper.delete_namespace(self.namespace_name)
        self._delete_account()
        return True

    def ensure_share(self):
        if not self.share.get('export_locations') or not self.share.get('export_locations')[0].get('path'):
            err_msg = _(" share fail for invalid export location.")
            raise exception.InvalidShare(reason=err_msg)

        result = self._get_namespace_info()
        self._check_namespace_running_status(result)
        return self._get_ensure_share_location()

    def change_share(self, new_size, action):

        if not self.share.get('export_locations') or not self.share.get('export_locations')[0].get('path'):
            err_msg = _("share fail for invalid export location.")
            raise exception.InvalidShare(reason=err_msg)

        namespace_info = self._get_namespace_info()
        self._get_quota_info(namespace_info, action, self.namespace_id,
                             new_size, constants.QUOTA_PARENT_TYPE_NAMESPACE)
        self.helper.change_quota_size(self.quota_id, new_size)
        LOG.info("{0} share done. New size:{1}.".format(action, new_size))
        return True

    def _check_domain(self):
        """当共享协议类型存在Nfs或Cifs时，检查配置文件集群域名是否存在"""

        self.domain = self.root.findtext('Filesystem/ClusterDomainName').strip()
        if ('NFS' in self.share_proto or 'CIFS' in self.share_proto) and not self.domain:
            err_msg = _("Create namespace({0}) error, because can't "
                        "get the domain name of cluster...".format(self.share['id']))
            raise exception.InvalidInput(err_msg)

    def _get_tier_info(self):
        """从manila提供的share_types中获取分级策略信息"""

        type_id = self.share.get('share_type_id')
        extra_specs = share_types.get_share_type_extra_specs(type_id)

        self.tier_info['enable_tier'] = None
        if extra_specs:
            self.tier_info['enable_tier'] = strutils.bool_from_string(extra_specs.get('pacific:enable_tier'))

        if self.tier_info['enable_tier']:

            tmp_strategy = extra_specs.get('pacific:tier_strategy')
            if tmp_strategy \
                    and (tmp_strategy.strip() in (constants.TIER_GRADE_HOT,
                                                  constants.TIER_GRADE_WARM,
                                                  constants.TIER_GRADE_COLD)):
                self.tier_info['strategy'] = int(tmp_strategy.strip())
            else:
                err_msg = (_("The <pacific:tier_strategy> in share type must be set as 0 or 1 or 2."))
                raise exception.InvalidInput(reason=err_msg)

            tmp_mtime = extra_specs.get('pacific:tier_time')
            if tmp_mtime and tmp_mtime.strip().isdigit() \
                    and int(tmp_mtime.strip()) in range(0, constants.MTIME_MAX + 1):
                self.tier_info['mtime'] = int(tmp_mtime.strip())
            else:
                err_msg = (_("The <pacific:tier_time> in share type must be int([0, 1096])."))
                raise exception.InvalidInput(reason=err_msg)

            tmp_atime_mode = extra_specs.get('pacific:atime_mode')
            if tmp_atime_mode and tmp_atime_mode.strip().isdigit() \
                    and (int(tmp_atime_mode.strip()) in (3600, 86400)):
                self.tier_info['atime_mode'] = int(tmp_atime_mode.strip())
            else:
                err_msg = (_("The <pacific:atime_mode> in share type must be set as 3600 or 86400."))
                raise exception.InvalidInput(reason=err_msg)
        else:
            self.tier_info['strategy'] = None
            self.tier_info['mtime'] = None
            self.tier_info['atime_mode'] = constants.ATIME_UPDATE_CLOSE

    def _get_max_band_width_qos_config(self, extra_specs):
        tmp_max_band_width = extra_specs.get('pacific:max_band_width')
        if tmp_max_band_width is None:
            self.qos_config['max_band_width'] = constants.MAX_BAND_WIDTH
        elif tmp_max_band_width.strip().isdigit() \
                and 1 <= int(tmp_max_band_width.strip()) <= constants.BAND_WIDTH_UPPER_LIMIT:
            self.qos_config['max_band_width'] = int(tmp_max_band_width.strip())
        else:
            err_msg = _("The <pacific:max_band_width> in share type "
                        "must be int([1, %s]).") % constants.BAND_WIDTH_UPPER_LIMIT
            raise exception.InvalidInput(reason=err_msg)

    def _get_max_iops_qos_config(self, extra_specs):
        tmp_max_iops = extra_specs.get('pacific:max_iops')
        if tmp_max_iops is None:
            self.qos_config['max_iops'] = constants.MAX_IOPS
        elif tmp_max_iops.strip().isdigit() \
                and 0 <= int(tmp_max_iops.strip()) <= constants.MAX_IOPS_UPPER_LIMIT:
            self.qos_config['max_iops'] = int(tmp_max_iops.strip())
        else:
            err_msg = _("The <pacific:max_iops> in share type "
                        "must be int([0, %s]).") % constants.MAX_IOPS_UPPER_LIMIT
            raise exception.InvalidInput(reason=err_msg)

    def _get_basic_band_width_qos_config(self, extra_specs):
        tmp_basic_band_width = extra_specs.get('pacific:basic_band_width')
        if tmp_basic_band_width is None:
            self.qos_config['basic_band_width'] = constants.BASIC_BAND_WIDTH
        elif tmp_basic_band_width.strip().isdigit() \
                and 1 <= int(tmp_basic_band_width.strip()) <= constants.BAND_WIDTH_UPPER_LIMIT:
            self.qos_config['basic_band_width'] = int(tmp_basic_band_width.strip())
        else:
            err_msg = _("The <pacific:basic_band_width> in share type "
                        "must be int([1, %s]).") % constants.BAND_WIDTH_UPPER_LIMIT
            raise exception.InvalidInput(reason=err_msg)

    def _get_bps_density_qos_config(self, extra_specs):
        tmp_bps_density = extra_specs.get('pacific:bps_density')
        if tmp_bps_density is None:
            self.qos_config['bps_density'] = constants.BPS_DENSITY
        elif tmp_bps_density.strip().isdigit() \
                and 1 <= int(tmp_bps_density.strip()) <= constants.MAX_BPS_DENSITY:
            self.qos_config['bps_density'] = int(tmp_bps_density.strip())
        else:
            err_msg = _("The <pacific:bps_density> in share type "
                        "must be int([1, %s]).") % constants.MAX_BPS_DENSITY
            raise exception.InvalidInput(reason=err_msg)

    def _get_qos_config(self):
        """从manila提供的share_types中获取qos策略信息"""

        type_id = self.share.get('share_type_id')
        extra_specs = share_types.get_share_type_extra_specs(type_id)

        self._get_max_band_width_qos_config(extra_specs)
        self._get_max_iops_qos_config(extra_specs)
        self._get_basic_band_width_qos_config(extra_specs)
        self._get_bps_density_qos_config(extra_specs)

    def _find_account_name(self, root=None):
        return self.share.get("project_id")

    def _get_or_create_account(self):
        """
        Driver在创建文件系统时先查询projectId对应的租户是否存在，如果存在，直接使用租户Id。
        如果不存在，创建一个租户，租户名称为公有云下发的projectId，返回的租户accountId供系统内部使用。
        """

        account_name = self._find_account_name(self.root)
        result = self.helper.query_account_by_name(account_name)
        if result:
            self.account_id = result.get('id')
            LOG.info("Account({0}) already exist. No need create.".format(account_name))
        else:
            result = self.helper.create_account(account_name)
            self.account_id = result.get('id')

    def _create_namespace_find_namespace_name(self, share_name):
        self.namespace_name = share_name
        for i in range(1, 12):
            result = self.helper.query_namespace_by_name(self.namespace_name)
            if result:
                LOG.info(_("Namespace({0}) has been used, Try to find other".format(self.namespace_name)))
                self.namespace_name = share_name + '_{0:0>2d}'.format(i)
                if i == 11:
                    err_msg = _("Duplicate namespace:{0} (_01~10).".format(self.namespace_name))
                    raise exception.InvalidInput(reason=err_msg)
            else:
                return

    def _create_namespace(self):
        """
        在对应账户下创建命名空间
        命名空间名称首先取用户指定的名称，如果用户取指定去share_instance_id
        """

        if self.share['display_name']:
            self.namespace_name = 'share-' + self.share.get('display_name')
        else:
            self.namespace_name = 'share-' + self.share.get('id')

        try:
            forbidden_dpc = constants.SUPPORT_DPC if 'DPC' in self.share_proto else constants.NOT_SUPPORT_DPC
            storage_pool_id = self.free_pool[0]
            result = self.helper.create_namespace(self.namespace_name, storage_pool_id, self.account_id,
                                                  forbidden_dpc, self.tier_info['atime_mode'])
            self.namespace_id = result.get('id')
        except Exception as e:
            self._rollback_creat(1)
            raise e

    def _create_quote(self):
        """创建命名空间配额"""

        quota_size = self.share['size']
        try:
            self.helper.creat_quota(self.namespace_id, quota_size,
                                    constants.QUOTA_PARENT_TYPE_NAMESPACE)
        except Exception as e:
            self._rollback_creat(1)
            raise e

    def _create_qos(self):
        """创建qos策略并关联到对应的命名空间，qos名称和命名空间名称相同"""

        qos_name = self.namespace_name
        try:
            result = self.helper.create_qos(qos_name, self.account_id, self.qos_config)
            qos_policy_id = result.get('id')
            self.helper.add_qos_association(self.namespace_name, qos_policy_id, self.account_id)
        except Exception as e:
            self._rollback_creat(2)
            raise e

    def _create_tier_migrate_policy(self):
        """创建迁移策略，迁移策略名称和命名空间名称相同"""

        tier_name = self.namespace_name
        enable_tier = self.tier_info['enable_tier']
        strategy = self.tier_info['strategy']
        mtime = self.tier_info['mtime']
        if enable_tier:
            try:
                self.helper.add_tier_policy(tier_name, self.namespace_id, strategy, True, mtime)
                self.helper.add_tier_policy(tier_name, self.namespace_id, strategy, False, mtime)
                self.helper.add_tier_migrate_schedule(self.namespace_id)
            except Exception as e:
                self._rollback_creat(2)
                raise e

    def _create_share_protocol(self):
        try:
            if 'NFS' in self.share_proto:
                self.helper.create_nfs_share(self.namespace_name, self.account_id)
            if 'CIFS' in self.share_proto:
                self.helper.create_cifs_share(self.namespace_name, self.account_id)
        except Exception as e:
            self._rollback_creat(3)
            raise e

    def _rollback_creat(self, level):

        LOG.error(_("Try to rollback..."))
        if level >= 3:
            self._delete_share_protocol()
        if level >= 2:
            self.helper.delete_qos(self.namespace_name)
        if level >= 1:
            self.helper.delete_namespace(self.namespace_name)
            self._delete_account()
        LOG.info(_("Rollback done."))

    def _delete_share_protocol(self):

        if 'NFS' in self.share_proto:
            result = self.helper.query_nfs_share_information(self.account_id)
            for nfs_share in result:
                if str(self.namespace_id) == nfs_share['file_system_id']:
                    nfs_share_id = nfs_share.get('id')
                    self.helper.delete_nfs_share(nfs_share_id, self.account_id)
                    break
        if 'CIFS' in self.share_proto:
            result = self.helper.query_cifs_share_information(self.account_id)
            for cifs_share in result:
                if str(self.namespace_id) == cifs_share['file_system_id']:
                    cifs_share_id = cifs_share.get('id')
                    self.helper.delete_cifs_share(cifs_share_id, self.account_id)
                    break

    def _delete_account(self):

        result = self.helper.query_namespaces_count(self.account_id)
        namespace_count = result['count']
        LOG.info(_("Account has {0} namespaces".format(namespace_count)))
        result = self.helper.query_access_zone_count(self.account_id)
        access_zone_count = result['count']
        LOG.info(_("Account has {0} access zone".format(access_zone_count)))

        if not (namespace_count or access_zone_count):
            LOG.info("The account has no namespace and access zone. "
                     "Try to delete.(account_id: {0})".format(self.account_id))
            result_query_users = self.helper.query_users_by_id(self.account_id)
            for user in result_query_users:
                user_name = user['name']
                self.helper.delete_unix_user(user_name, self.account_id)

            result_query_usergroups = self.helper.query_user_groups_by_id(self.account_id)
            for group in result_query_usergroups:
                group_name = group['name']
                self.helper.delete_unix_user_group(group_name, self.account_id)
            self.helper.delete_account(self.account_id)
        else:
            LOG.info("The account has namespace or access zone. "
                     "Cannot delete.(account_id: {0})".format(self.account_id))

    def _get_account_id(self):
        """通过账户名称去存储查询账户信息并获取账户ID"""
        account_name = self._find_account_name(self.root)
        result = self.helper.query_account_by_name(account_name)
        self.account_id = result.get('id')

    def _get_namespace_info(self):
        """
        先通过share的location获取namespace名称，
        再根据namespace名称查询namespace信息并获取namespace id
        """
        self.export_locations = self.share.get('export_locations')[0].get('path')
        self.namespace_name = self.export_locations.split('\\')[-1].split('/')[-1]
        result = self.helper.query_namespace_by_name(self.namespace_name)
        self.namespace_id = result.get('id')
        return result

    def _check_namespace_running_status(self, result):
        status = result.get('running_status')
        if status != 0:
            err_msg = _("The running status of share({0}) is not normal.".format(self.namespace_name))
            raise exception.InvalidShare(reason=err_msg)

    def _get_ensure_share_location(self):
        location = []
        for export_location in self.share.get('export_locations'):
            location.append(export_location.get('path'))

        return location

    def _get_location(self):
        """返回共享路径"""
        location = []
        if 'DPC' in self.share_proto:
            location.append('DPC:/' + self.namespace_name)
        if 'NFS' in self.share_proto:
            location.append('NFS:' + self.domain + ":/" + self.namespace_name)
        if 'CIFS' in self.share_proto:
            location.append('CIFS:\\\\' + self.domain + '\\' + self.namespace_name)
        if 'HDFS' in self.share_proto:
            location.append('HDFS:/' + self.namespace_name)

        return location

    def _get_quota_info(self, namespace_info, action, parent_id, new_size, parent_type):
        cur_size = float(namespace_info.get('space_used')) / constants.CAPACITY_UNIT_KB_TO_GB
        cur_size = math.ceil(cur_size)

        result = self.helper.query_quota_by_parent(parent_id, parent_type)
        self.quota_id = result.get('id')

        action = action.title()
        if (action == 'Shrink') and (cur_size > new_size):
            err_msg = (_("Shrink share fail for space used({0}G) > new sizre({1}G)".format(cur_size, new_size)))
            raise exception.InvalidInput(reason=err_msg)
