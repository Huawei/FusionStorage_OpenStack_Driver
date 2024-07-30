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

import math
from oslo_log import log
from oslo_utils import strutils

from manila import exception
from manila.i18n import _
from manila.share import api
from manila.share import share_types

from ..operate_share import OperateShare
from ...utils import constants, driver_utils

LOG = log.getLogger(__name__)
share_api = api.API()


class CommunityOperateShare(OperateShare):

    def __init__(self, client, share=None, driver_config=None,
                 context=None, storage_features=None):
        super(CommunityOperateShare, self).__init__(
            client, share, driver_config, context, storage_features)

        self.domain = None  # 集群域名
        self.namespace_name = None  # 命名空间名称
        self.namespace_id = None  # 命名空间Id
        self.export_locations = None  # share路径信息
        self.quota_id = None  # 配额ID
        self.tier_info = {}  # 分级策略信息
        self.qos_config = {}  # QOS策略信息

    @staticmethod
    def get_impl_type():
        return constants.PLUGIN_COMMUNITY_IMPL

    @staticmethod
    def _check_and_get_share_capacity(share_data):
        if share_data.get("space_used") is None:
            return {}

        hard_limit = share_data.get("space_hard_quota")
        used_space = share_data.get("space_used")

        share_capacity = {
            "hard_limit": str(hard_limit),
            "used_space": str(used_space),
            "avail_space": str(hard_limit - used_space)
        }

        if share_data.get('ssd_hard_quota') is None:
            LOG.info("Share has no ssd quota, don't need return.")
            return share_capacity

        # get share tier capacity
        ssd_hard_limit = share_data.get("ssd_hard_quota")
        ssd_used_space = share_data.get("ssd_space_used")
        hdd_hard_limit = share_data.get("hdd_hard_quota")
        hdd_used_space = share_data.get("hdd_space_used")
        share_capacity.update({
            'ssd_hard_limit': str(ssd_hard_limit),
            'ssd_used_space': str(ssd_used_space),
            'ssd_avail_space': str(ssd_hard_limit - ssd_used_space),
            'hdd_hard_limit': str(hdd_hard_limit),
            'hdd_used_space': str(hdd_used_space),
            'hdd_avail_space': str(hdd_hard_limit - hdd_used_space)
        })

        return share_capacity

    def create_share(self):
        self._check_domain()
        self._get_qos_config()
        self._get_or_create_account()
        self._create_namespace()
        self._create_quota()
        self._create_qos()
        self._check_and_create_tier_policy()
        self._create_share_protocol()
        return self._get_location()

    def delete_share(self):
        if (not self.share.get('export_locations') or not self.share.get(
                'export_locations')[0].get('path')):
            LOG.warn(_("Delete share fail for invalid export location."))
            return False

        self._get_account_id()
        if not self._get_namespace_info():
            LOG.warn(_("Delete share fail, cannot find namespace info of share"))
            return False
        self._delete_share_protocol()
        self.client.delete_qos(self.namespace_name)
        self.client.delete_namespace(self.namespace_name)
        self._delete_account()
        return True

    def ensure_share(self):
        if (not self.share.get('export_locations') or not self.share.get(
                'export_locations')[0].get('path')):
            err_msg = _("Ensure share fail for invalid export location.")
            raise exception.InvalidShare(reason=err_msg)

        result = self._get_namespace_info()
        self._check_namespace_running_status(result)
        return self._get_ensure_share_location()

    def change_share(self, new_size, action):

        if (not self.share.get('export_locations') or not self.share.get(
                'export_locations')[0].get('path')):
            err_msg = _("Change share fail for invalid export location.")
            raise exception.InvalidShare(reason=err_msg)

        namespace_info = self._get_namespace_info()
        self._check_and_update_tier_size(namespace_info, new_size)
        self._get_quota_info(namespace_info, action, self.namespace_id,
                             new_size, constants.QUOTA_PARENT_TYPE_NAMESPACE)
        self.client.change_quota_size(self.quota_id, new_size)
        LOG.info("{0} share done. New size:{1}.".format(action, new_size))
        return True

    def get_share_usage(self, share_usages):
        pass

    def update_qos(self, qos_specs):
        pass

    def parse_cmcc_qos_options(self):
        """解冻前需要先获取要恢复的qos信息"""
        share_qos_info = {
            "total_bytes_sec": 0,
            "total_iops_sec": 0
        }
        return share_qos_info

    def _check_domain(self):
        """当共享协议类型存在Nfs或Cifs时，检查配置文件集群域名是否存在"""

        domain_name = self.driver_config.domain
        self.domain = domain_name.strip() if domain_name else domain_name
        if ('NFS' in self.share_proto or 'CIFS' in self.share_proto) and not self.domain:
            err_msg = _("Create namespace({0}) error, because can't "
                        "get the domain name of cluster...".format(self.share['id']))
            raise exception.InvalidInput(err_msg)

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

    def _get_or_create_account(self):
        """
        Driver在创建文件系统时先查询projectId对应的租户是否存在，如果存在，直接使用租户Id。
        如果不存在，创建一个租户，租户名称为公有云下发的projectId，返回的租户accountId供系统内部使用。
        """
        self._get_account_id()
        if self.account_id is not None:
            LOG.info("Account({0}) already exist. No need create.".format(self.account_name))
        else:
            LOG.info("Begin to create account, account name is %s", self.account_name)
            result = self.client.create_account(self.account_name)
            self.account_id = result.get('id')

    def _set_namespace_param(self):
        self.namespace_name = 'share-' + self.share.get('share_id')
        param_dict = {
            'name': self.namespace_name,
            'forbidden_dpc': self._get_forbidden_dpc_param(),
            'storage_pool_id': self.storage_pool_id,
            'account_id': self.account_id,
            'atime_update_mode': constants.ATIME_UPDATE_HOURS,
            'case_sensitive': constants.CASE_INSENSITIVE
        }
        self.tier_info = self._get_all_share_tier_policy()
        hot_data_size = self.tier_info.get('hot_data_size')
        total_size = self.share.get('size')
        if hot_data_size is None:
            return param_dict
        hot_data_size = int(hot_data_size)
        if hot_data_size > total_size:
            LOG.warning("the configured hot data size %s is bigger than total size, "
                        "set it to total siz %s", hot_data_size, total_size)
            hot_data_size = total_size
        param_dict.update({
            'tier_hot_cap_limit': driver_utils.capacity_unit_up_conversion(
                hot_data_size, constants.BASE_VALUE, constants.POWER_BETWEEN_KB_AND_GB),
            'tier_cold_cap_limit': driver_utils.capacity_unit_up_conversion(
                total_size - hot_data_size,
                constants.BASE_VALUE, constants.POWER_BETWEEN_KB_AND_GB
            )
        })
        return param_dict

    def _create_namespace(self):
        """
        在对应账户下创建命名空间,DPC场景下需要打开DPC鉴权开关
        """

        self.storage_pool_id = self._get_current_storage_pool_id()
        namespace_param = self._set_namespace_param()
        try:
            LOG.info("Begin to create namespace, namespace name is %s", self.namespace_name)
            result = self.client.create_namespace(namespace_param)
            self.namespace_id = result.get('id')
            if 'DPC' in self.share_proto:
                LOG.info("Begin to open dpc auth switch, namespace name is %s",
                         self.namespace_name)
                self.client.open_dpc_auth_switch(self.namespace_name)
        except Exception as e:
            self._rollback_creat(1)
            raise e

    def _create_quota(self):
        """创建命名空间配额"""

        quota_size = self.share['size']
        try:
            LOG.info("Begin to create namespace quota, namespace name is %s",
                     self.namespace_name)
            self.client.creat_quota(self.namespace_id, quota_size,
                                    constants.QUOTA_PARENT_TYPE_NAMESPACE)
        except Exception as e:
            self._rollback_creat(1)
            raise e

    def _create_qos(self):
        """创建qos策略并关联到对应的命名空间，qos名称和命名空间名称相同"""

        qos_name = self.namespace_name
        try:
            LOG.info("Begin to create qos, qos name is %s",
                     self.namespace_name)
            result = self.client.create_qos(qos_name, self.account_id, self.qos_config)
            qos_policy_id = result.get('id')
            LOG.info("Begin to associate qos to namespace, namespace name is %s",
                     self.namespace_name)
            self.client.add_qos_association(self.namespace_name, qos_policy_id, self.account_id)
        except Exception as e:
            self._rollback_creat(2)
            raise e

    def _check_and_create_tier_policy(self):
        tier_grade_param, tier_migrate_param = self._check_and_get_tier_param()
        # create tier grade param
        if tier_grade_param:
            try:
                LOG.info("Begin to create tier grade policy to namespace,"
                         " namespace name is %s", self.namespace_name)
                self.client.create_tier_grade_policy(tier_grade_param)
            except Exception as e:
                self._rollback_creat(3)
                raise e

        # create tier migrate periodicity policy
        if tier_migrate_param:
            try:
                LOG.info("Begin to create tier migrate policy to namespace,"
                         " namespace name is %s", self.namespace_name)
                self.client.create_tier_migrate_policy(tier_migrate_param)
            except Exception as e:
                self._rollback_creat(4)
                raise e

    def _create_share_protocol(self):
        try:
            if 'NFS' in self.share_proto:
                LOG.info("Begin to create NFS share to namespace,"
                         " namespace name is %s", self.namespace_name)
                self.client.create_nfs_share(self.namespace_name, self.account_id)
            if 'CIFS' in self.share_proto:
                LOG.info("Begin to create CIFS share to namespace,"
                         " namespace name is %s", self.namespace_name)
                self.client.create_cifs_share(self.namespace_name, self.account_id)
        except Exception as e:
            self._rollback_creat(5)
            raise e

    def _check_and_update_tier_size(self, namespace_info, new_size):
        """
        check is need to update hot tier size
        :param namespace_info: current namespace info
        :param new_size: final total capacity of namesapce
        :return: None
        """
        self.tier_info = self._get_all_share_tier_policy()
        current_hot_data_size = namespace_info.get('tier_hot_cap_limit', 0)
        new_hot_data_size = self.tier_info.get('hot_data_size')

        if new_hot_data_size is None:
            return

        new_cold_data_size = driver_utils.capacity_unit_up_conversion(
            new_size - int(new_hot_data_size), constants.BASE_VALUE, 2)
        new_hot_data_size = driver_utils.capacity_unit_up_conversion(
            int(new_hot_data_size), constants.BASE_VALUE, 2)
        total_increase_capacity = driver_utils.capacity_unit_up_conversion(
            new_size - self.share.get('size'), constants.BASE_VALUE, 2)
        hot_increase_capacity = new_hot_data_size - current_hot_data_size

        if hot_increase_capacity > total_increase_capacity:
            err_msg = (_("Extend hot data size failed, hot_increase_capacity %sKB can "
                         "not bigger than total_increase_capacity %sKB") %
                       (hot_increase_capacity, total_increase_capacity))
            LOG.error(err_msg)
            raise exception.InvalidInput(reason=err_msg)

        update_param = {
            'id': self.namespace_id,
            'tier_hot_cap_limit': new_hot_data_size,
            'tier_cold_cap_limit': new_cold_data_size
        }
        self.client.change_namespace_info(update_param)

    def _rollback_creat(self, level):

        LOG.error(_("Try to rollback..."))
        if level >= 5:
            self._delete_share_protocol()
        if level >= 4:
            self.client.delete_tier_migrate_policy_by_name(
                self.namespace_name + constants.PERIODICITY_NAME,
                self.namespace_id, self.account_id)
        if level >= 3:
            self.client.delete_tier_grade_policy_by_name(
                self.namespace_name + constants.GRADE_NAME,
                self.namespace_id, self.account_id)
        if level >= 2:
            self.client.delete_qos(self.namespace_name)
        if level >= 1:
            self.client.delete_namespace(self.namespace_name)
            self._delete_account()
        LOG.info(_("Rollback done."))

    def _delete_share_protocol(self):
        if 'NFS' in self.share_proto:
            result = self.client.query_nfs_share_information(self.account_id, self.namespace_id)
            for nfs_share in result:
                if str(self.namespace_id) == nfs_share['file_system_id']:
                    nfs_share_id = nfs_share.get('id')
                    self.client.delete_nfs_share(nfs_share_id, self.account_id)
                    break
        if 'CIFS' in self.share_proto:
            result = self.client.query_cifs_share_information(
                self.account_id, self.namespace_name)
            for cifs_share in result:
                if str(self.namespace_id) == cifs_share['file_system_id']:
                    cifs_share_id = cifs_share.get('id')
                    self.client.delete_cifs_share(cifs_share_id, self.account_id)
                    break

    def _delete_account(self):
        result = self.client.query_namespaces_count(self.account_id)
        namespace_count = result['count']
        LOG.info(_("Account has {0} namespaces".format(namespace_count)))
        result = self.client.query_access_zone_count(self.account_id)
        access_zone_count = result['count']
        LOG.info(_("Account has {0} access zone".format(access_zone_count)))

        if not (namespace_count or access_zone_count):
            LOG.info("The account has no namespace and access zone. "
                     "Try to delete.(account_id: {0})".format(self.account_id))
            result_query_users = self.client.query_users_by_id(self.account_id)
            for user in result_query_users:
                user_name = user['name']
                self.client.delete_unix_user(user_name, self.account_id)

            result_query_usergroups = self.client.query_user_groups_by_id(self.account_id)
            for group in result_query_usergroups:
                group_name = group['name']
                self.client.delete_unix_user_group(group_name, self.account_id)
            self.client.delete_account(self.account_id)
        else:
            LOG.info("The account has namespace or access zone. "
                     "Cannot delete.(account_id: {0})".format(self.account_id))

    def _get_namespace_info(self):
        """
        先通过share的location获取namespace名称，
        再根据namespace名称查询namespace信息并获取namespace id
        """
        self.export_locations = self.share.get('export_locations')[0].get('path')
        self.namespace_name = self.export_locations.split('\\')[-1].split('/')[-1]
        result = self.client.query_namespace_by_name(self.namespace_name)
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
            dpc_path = self._get_dpc_path('/' + self.namespace_name)
            location.append('DPC:' + dpc_path)
        if 'NFS' in self.share_proto:
            nfs_path = self._get_nfs_path(self.domain + ":/" + self.namespace_name)
            location.append('NFS:' + nfs_path)
        if 'CIFS' in self.share_proto:
            location.append('CIFS:\\\\' + self.domain + '\\' + self.namespace_name)
        if 'HDFS' in self.share_proto:
            location.append('HDFS:/' + self.namespace_name)

        return location

    def _get_quota_info(self, namespace_info, action, parent_id, new_size, parent_type):
        cur_size = float(namespace_info.get('space_used')) / constants.CAPACITY_UNIT_KB_TO_GB
        cur_size = math.ceil(cur_size)

        result = self.client.query_quota_by_parent(parent_id, parent_type)
        self.quota_id = result.get('id')

        action = action.title()
        if (action == 'Shrink') and (cur_size > new_size):
            err_msg = (_("Shrink share fail for space used({0}G) > new sizre({1}G)".format(cur_size, new_size)))
            raise exception.InvalidInput(reason=err_msg)

    def _check_and_get_tier_param(self):
        """
        check storage is support tier to
        decided the create param of tier policy
        :return: tuple of tier grade and migrate policy
        """
        tier_grade_param, tier_migrate_param = {}, {}
        LOG.info("storage_features %s" % self.storage_features)
        current_tier_types = self.storage_features.get(
            self.storage_pool_name, {}).get('support_tier_types', [])
        # if current storage pool don't support tier, don't create tier policy
        if len(current_tier_types) <= 1:
            LOG.warning("Storage pool tier types only have one level %s,"
                        "Try to create tier policy.",
                        current_tier_types)

        self._get_tier_grade_param(tier_grade_param)
        self._get_tier_migrate_param(tier_migrate_param, current_tier_types)
        return tier_grade_param, tier_migrate_param

    def _get_tier_grade_param(self, tier_grade_param):
        """
        get tier grade create param
        :param tier_grade_param: empty dict
        :return: final tier param dict
        """
        tier_grade = self.tier_info.get('tier_place')
        if not tier_grade:
            LOG.info("Tier place policy didn't configure, Don't need create")
            return tier_grade_param

        tier_grade_param.update({
            'name': self.namespace_name + constants.GRADE_NAME,
            'fs_id': self.namespace_id,
            'strategy': driver_utils.convert_value_to_key(
                constants.TIER_ENUM, tier_grade.lower()),
            'account_id': self.account_id
        })
        return tier_grade_param

    def _get_tier_migrate_param(self, tier_migrate_param, current_tier_types):
        """
        get tier migrate create param
        :param tier_migrate_param: empty dict
        :param current_tier_types: storage tier type list
        :return: final tier param dict
        """
        tier_migrate_expiration = self.tier_info.get('tier_migrate_expiration')
        if not tier_migrate_expiration:
            LOG.info("Tier tier migrate expiration didn't configure, Don't need create")
            return tier_migrate_param

        tier_migrate_param.update({
            'name': self.namespace_name + constants.PERIODICITY_NAME,
            'fs_id': self.namespace_id,
            'strategy': driver_utils.convert_value_to_key(
                constants.TIER_ENUM, self.get_lowest_tier_grade(current_tier_types)),
            'account_id': self.account_id,
            'migration_type': constants.PERIODIC_MIGRATION_POLICY,
            'atime': int(tier_migrate_expiration),
            'atime_unit': constants.HTIME_UNIT,
            'atime_operator': constants.MATCH_RULE_GT
        })
        return tier_migrate_param

    def _get_dpc_path(self, share_path):
        """
        Combine the DPC mount path to be returned with options.
        Supported Customizations Options:
        cid={sn}
        :param share_path:
        :return:
        """
        dpc_mount_options = self.driver_config.dpc_mount_option
        if not dpc_mount_options:
            return share_path
        format_dict = {
            'sn': self.storage_features.get('sn')
        }
        final_path_param_list = [
            '-o', dpc_mount_options.format(**format_dict), share_path
        ]
        return ' '.join(final_path_param_list)

    def _get_nfs_path(self, share_path):
        """
        Combine the NFS mount path to be returned with options.
        :param share_path:
        :return:
        """
        nfs_mount_options = self.driver_config.nfs_mount_option
        if not nfs_mount_options:
            return share_path
        final_path_param_list = ['-o', nfs_mount_options, share_path]
        return ' '.join(final_path_param_list)

    def _get_update_qos_config(self, qos_specs):
        # total_bytes_sec and total_iops_sec must be exist
        if qos_specs.get('total_bytes_sec') is None or \
                qos_specs.get('total_iops_sec') is None:
            err_msg = "Can not get qos config when update_qos," \
                      "total_bytes_sec and total_iops_sec must need to be " \
                      "set when update qos" \
                      " the qos_specs is {0}".format(qos_specs)
            LOG.error(err_msg)
            raise exception.InvalidShare(reason=err_msg)

        # total_bytes_sec and total_iops_sec must be integer
        tmp_max_band_width = str(qos_specs.get('total_bytes_sec')).strip()
        tmp_max_iops = str(qos_specs.get('total_iops_sec')).strip()
        if not (tmp_max_band_width.isdigit() and tmp_max_iops.isdigit()):
            err_msg = "total_bytes_sec and total_iops_sec must be integer, " \
                      "the qos_specs is {0}".format(qos_specs)
            LOG.error(err_msg)
            raise exception.InvalidShare(reason=err_msg)

        self.qos_config['max_band_width'] = int(math.ceil(
            driver_utils.capacity_unit_down_conversion(
                float(tmp_max_band_width), constants.BASE_VALUE,
                constants.POWER_BETWEEN_BYTE_AND_MB)
        ))
        self.qos_config['max_iops'] = int(tmp_max_iops)
