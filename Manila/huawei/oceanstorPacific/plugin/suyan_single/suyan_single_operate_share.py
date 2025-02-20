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
import json
import math

from oslo_log import log

from manila import exception
from manila.i18n import _

from ..community.community_operate_share import CommunityOperateShare
from ...utils import constants, driver_utils

LOG = log.getLogger(__name__)


class SuyanSingleOperateShare(CommunityOperateShare):
    def __init__(self, client, share=None, driver_config=None,
                 context=None, storage_features=None):
        super(SuyanSingleOperateShare, self).__init__(
            client, share, driver_config, context, storage_features)
        self.share_parent_id = self._get_share_parent_id()
        self.dtree_name = None
        self.dtree_id = None
        self.enable_qos = True
        self.enable_tier = True

    @staticmethod
    def get_impl_type():
        return constants.PLUGIN_SUYAN_SINGLE_IMPL, None

    def create_share(self):
        if not self.share_parent_id:
            return super(SuyanSingleOperateShare, self).create_share()

        self._check_domain()
        self._get_or_create_account()
        self._get_share_parent_info()
        self._create_dtree()
        self._create_dtree_quota()
        self._create_dtree_share_protocol()
        return self._get_dtree_location()

    def change_share(self, new_size, action):
        if not self.share_parent_id:
            return super(SuyanSingleOperateShare, self).change_share(new_size, action)

        if (not self.share.get('export_locations') or not self.share.get(
                'export_locations')[0].get('path')):
            err_msg = _("change share fail for invalid export location.")
            raise exception.InvalidShare(reason=err_msg)

        self._get_dtree_namespace_info()
        self._get_dtree_info()
        self._get_dtree_quota_info(action, self.dtree_id,
                                   new_size, constants.QUOTA_PARENT_TYPE_DTREE)
        self.client.change_quota_size(self.quota_id, new_size)
        LOG.info("{0} share done. New size:{1}.".format(action, new_size))
        return True

    def update_qos(self, qos_specs):
        """
        苏研定制接口，根据传递的qos_specs，刷新share的qos信息，
        如果没有则创建对应qos, 此接口的share不是share_instance对象是share对象
        """

        self._set_share_to_share_instance()
        if (not self.share.get('export_locations') or not self.share.get(
                'export_locations')[0].get('path')):
            err_msg = _("update share qos fail for invalid export location.")
            raise exception.InvalidShare(reason=err_msg)

        self._get_update_qos_config(qos_specs)

        self._get_account_id()
        self._get_parent_name_from_export_locations()
        self._operate_share_qos(self.namespace_name, self.qos_config)

    def get_share_usage(self, share_usages):
        """苏研定制接口，通过share_usages获取对应share的容量信息"""

        return self._get_share_capacity(share_usages)

    def delete_share(self):
        if not self.share_parent_id:
            return super(SuyanSingleOperateShare, self).delete_share()

        if (not self.share.get('export_locations') or not self.share.get(
                'export_locations')[0].get('path')):
            LOG.warn(_("Delete share fail for invalid export location."))
            return False

        self._get_account_id()
        if not self._get_dtree_namespace_info():
            LOG.warn(_("Delete share fail, cannot find namespace info of share"))
            return False
        if not self._get_dtree_info():
            LOG.warn(_("Delete share fail, cannot find dtree info of share"))
            return False

        self._delete_dtree_share_protocol()
        self.client.delete_dtree(self.dtree_name, self.namespace_name)
        return True

    def ensure_share(self):
        if not self.share_parent_id:
            return super(SuyanSingleOperateShare, self).ensure_share()

        if (not self.share.get('export_locations') or not self.share.get(
                'export_locations')[0].get('path')):
            err_msg = _("Ensure share fail for invalid export location.")
            raise exception.InvalidShare(reason=err_msg)

        result = self._get_dtree_namespace_info()
        self._check_namespace_running_status(result)
        return self._get_ensure_share_location()

    def show_qos(self):
        try:
            self._set_share_to_share_instance()
        except Exception as err:
            LOG.warning("Show qos info failed,return {}, reason is %s", err)
            return {}

        export_locations = self.share.get('export_locations')
        if not export_locations or not export_locations[0].get('path'):
            LOG.warning("Show qos info failed for invalid export location, return {}"
                        "share export_locations is %s", export_locations)
            return {}

        self._get_account_id()
        self._get_parent_name_from_export_locations()
        return self._get_qos_param_of_namespace()

    def _get_parent_name_from_export_locations(self):
        """二级目录场景下获取namespace名称的方式有差异"""

        export_location = self.share.get('export_locations')[0].get('path')
        self._get_namespace_name_from_location(export_location)

    def _get_share_parent_info(self):
        """首先去存储查询父目录的命名空间信息，查询不到抛错"""

        self.namespace_name = 'share-' + self.share_parent_id
        namespace_info = self.client.query_namespace_by_name(self.namespace_name)
        if namespace_info:
            LOG.info(_("Namespace({0}) found successfully.".format(self.namespace_name)))
            self.namespace_id = namespace_info.get('id')
            return

        err_msg = _('Create Dtree failed, Can not fount parent share info %s on storage.') % self.share_parent_id
        LOG.error(err_msg)
        raise exception.InvalidInput(reason=err_msg)

    def _create_dtree(self):
        """创建二级目录的dtree"""

        self.dtree_name = 'share-' + self.share.get('share_id')
        try:
            result = self.client.create_dtree(self.dtree_name, self.namespace_name)
            self.dtree_id = result.get('id')
        except Exception as e:
            self._rollback_dtree_creat(1)
            raise e

    def _create_dtree_quota(self):
        """创建二级目录的配额"""

        quota_size = self.share.get('size')
        try:
            self.client.creat_quota(self.dtree_id, quota_size,
                                    constants.QUOTA_PARENT_TYPE_DTREE)
        except Exception as e:
            self._rollback_dtree_creat(1)
            raise e

    def _create_dtree_share_protocol(self):
        """创建二级目录共享"""

        try:
            if 'NFS' in self.share_proto:
                self.client.create_dtree_nfs_share(
                    self.namespace_name, self.dtree_name, self.account_id)
            if 'CIFS' in self.share_proto:
                self.client.create_dtree_cifs_share(
                    self.namespace_name, self.dtree_name, self.account_id)
        except Exception as e:
            self._rollback_dtree_creat(2)
            raise e

    def _rollback_dtree_creat(self, level):
        """当创建dtree过程中出现error，需要将前面已创建的对象清理掉"""

        LOG.error(_("Try to rollback..."))
        if level >= 2:
            self._delete_dtree_share_protocol()
        if level >= 1:
            self.client.delete_dtree(self.dtree_name, self.namespace_name)

        LOG.info(_("Rollback done."))

    def _delete_dtree_share_protocol(self):
        """
        二级目录场景下
        NFS根据dtree ID查询对应的共享信息
        CIFS根据dtree 名称查询对应的共享信息
        """

        if 'NFS' in self.share_proto:
            result = self.client.query_nfs_share_information(self.account_id, self.namespace_id, self.dtree_id)
            for nfs_share in result:
                if str(self.dtree_id) == nfs_share.get('dtree_id'):
                    nfs_share_id = nfs_share.get('id')
                    self.client.delete_nfs_share(nfs_share_id, self.account_id)
                    break
        if 'CIFS' in self.share_proto:
            result = self.client.query_cifs_share_information(
                self.account_id, self.dtree_name)
            for cifs_share in result:
                if str(self.dtree_name) == cifs_share.get('name'):
                    cifs_share_id = cifs_share.get('id')
                    self.client.delete_cifs_share(cifs_share_id, self.account_id)
                    break

    def _get_dtree_location(self):
        """返回二级目录的共享路径"""

        location = []
        share_path = self.namespace_name + '/' + self.dtree_name
        if 'NFS' in self.share_proto:
            nfs_path = self._get_nfs_path(self.domain + ":/" + share_path)
            location.append('NFS:' + nfs_path)
        if 'CIFS' in self.share_proto:
            location.append('CIFS:\\\\' + self.domain + '\\' + share_path)
        if 'DPC' in self.share_proto:
            dpc_path = self._get_dpc_path('/' + share_path)
            location.append('DPC:' + dpc_path)

        LOG.info("Create share successfully, the location of this share is %s", location)
        return location

    def _get_dtree_namespace_info(self):
        """二级目录场景下，通过location获取namespace名称后再去获取namespace信息"""

        self.export_locations = self.share.get('export_locations')[0].get('path')
        self.namespace_name = self.export_locations.split('\\')[-1].split('/')[-2]
        result = self.client.query_namespace_by_name(self.namespace_name)
        self.namespace_id = result.get('id')
        return result

    def _get_dtree_info(self):
        """二级目录场景下，通过location获取dtree名称后再去获取dtree信息"""

        self.export_locations = self.share.get('export_locations')[0].get('path')
        self.dtree_name = self.export_locations.split('\\')[-1].split('/')[-1]
        result = self.client.query_dtree_by_name(self.dtree_name, self.namespace_id)
        for dtree_info in result:
            self.dtree_id = dtree_info.get('id')
            return True

        return False

    def _get_share_capacity(self, share_usages):
        if not self.share.get('share_id'):
            err_msg = _("There is no share_id attribution in share object:%s") % self.share
            LOG.error(err_msg)
            raise exception.InvalidShare(reason=err_msg)

        share_usage = share_usages.get(self.share.get('share_id'), {})
        if not share_usage:
            LOG.info("Can not find share in share_usages. Try to get share capacity from storage")
            return self._get_share_capacity_from_storage()

        LOG.info("Get share usage:%s of share:%s from share_usages successfully",
                 share_usage, self.share.get('share_id'))
        return share_usage

    def _get_namespace_name_from_location(self, export_location):
        """
        when share_parent_id is exist, export_location like this:
        nfs_share: NFS:fake_logic_ip:/namespace_name/dtree_name
        cifs_share: CIFS:\\\\fake_logic_ip\\namespace_name/dtree_name
        else, export_location like this:
        nfs_share: NFS:fake_logic_ip:/namespace_name
        cifs_share: CIFS:\\\\fake_logic_ip\\namespace_name
        """
        if self.share_parent_id:
            self.namespace_name = export_location.split('\\')[-1].split('/')[-2]
            self.dtree_name = export_location.split('\\')[-1].split('/')[-1]
        else:
            self.namespace_name = export_location.split('\\')[-1].split('/')[-1]

    def _get_dtree_quota_info(self, action, parent_id, new_size, parent_type):
        if not parent_id:
            error_msg = (_("%s share fail for because of dtree not exist") % action)
            LOG.error(error_msg)
            raise exception.InvalidInput(reason=error_msg)

        dtree_quota = self.client.query_quota_by_parent(parent_id, parent_type)
        cur_size = float(dtree_quota.get('space_used', 0.0)) / constants.CAPACITY_UNIT_BYTE_TO_GB
        cur_size = math.ceil(cur_size)

        self.quota_id = dtree_quota.get('id')

        action = action.title()
        if (action == 'Shrink') and (cur_size > new_size):
            err_msg = (_("Shrink share fail for space used({0}G) > new sizre({1}G)".format(cur_size, new_size)))
            raise exception.InvalidInput(reason=err_msg)

    def _get_share_capacity_from_storage(self):
        if (not self.share.get('export_locations') or not self.share.get(
                'export_locations')[0].get('path')):
            LOG.error("Get namespace_name fail for invalid export location:%s.",
                      self.share.get('export_locations'))
            return {}

        self._get_parent_name_from_export_locations()

        share_capacity = {}
        namespace_info = self.client.query_namespace_by_name(self.namespace_name)
        if not namespace_info:
            err_msg = "Can not found namespace of share from storage, namespace name is %s." % self.namespace_name
            LOG.error(err_msg)
            return share_capacity

        self.namespace_id = namespace_info.get('id')
        if not self.share_parent_id:
            return self._get_namespace_capacity(share_capacity, namespace_info)
        return self._get_dtree_capacity(share_capacity)

    def _get_namespace_capacity(self, share_capacity, namespace_info):
        used_space = driver_utils.capacity_unit_up_conversion(
            namespace_info.get('space_used', 0), constants.BASE_VALUE, constants.POWER_BETWEEN_BYTE_AND_KB)
        hard_limit = driver_utils.capacity_unit_up_conversion(
            namespace_info.get('space_hard_quota', 0), constants.BASE_VALUE, constants.POWER_BETWEEN_BYTE_AND_KB)

        share_capacity.update({
            "hard_limit": str(hard_limit),
            "used_space": str(used_space),
            "avail_space": str(hard_limit - used_space)
        })

        tier_hot_cap_limit = namespace_info.get('tier_hot_cap_limit')
        tier_cold_cap_limit = namespace_info.get('tier_cold_cap_limit')
        if tier_hot_cap_limit is None and tier_cold_cap_limit is None:
            LOG.info("Get share usage:%s of namespace share:%s from storage successfully",
                     share_capacity, self.namespace_name)
            return share_capacity
        ssd_hard_limit = driver_utils.capacity_unit_up_conversion(
            tier_hot_cap_limit, constants.BASE_VALUE, constants.POWER_BETWEEN_BYTE_AND_KB)
        hdd_hard_limit = driver_utils.capacity_unit_up_conversion(
            tier_cold_cap_limit, constants.BASE_VALUE, constants.POWER_BETWEEN_BYTE_AND_KB)
        tier_perf_cap = json.loads(namespace_info.get('tier_perf_cap', '{}'))
        ssd_used_space = tier_perf_cap.get('hot', {}).get('used')
        hdd_used_space = tier_perf_cap.get('cold', {}).get('used')
        share_capacity.update({
            'ssd_hard_limit': str(ssd_hard_limit),
            'ssd_used_space': str(ssd_used_space),
            'ssd_avail_space': str(ssd_hard_limit - ssd_used_space),
            'hdd_hard_limit': str(hdd_hard_limit),
            'hdd_used_space': str(hdd_used_space),
            'hdd_avail_space': str(hdd_hard_limit - hdd_used_space)
        })
        LOG.info("Get share usage:%s of namespace share:%s from storage successfully",
                 share_capacity, self.namespace_name)
        return share_capacity

    def _get_dtree_capacity(self, share_capacity):
        dtree_info = self.client.query_dtree_by_name(self.dtree_name, self.namespace_id)
        if not dtree_info:
            err_msg = "Can not found dtree of share from storage, dtree name is %s." % self.dtree_name
            LOG.error(err_msg)
            return share_capacity

        for info in dtree_info:
            self.dtree_id = info.get('id')

        dtree_quota = self.client.query_quota_by_parent(
            self.dtree_id, constants.QUOTA_PARENT_TYPE_DTREE)
        used_space = dtree_quota.get('space_used', 0)
        hard_limit = dtree_quota.get('space_hard_quota', 0)
        share_capacity.update({
                "hard_limit": str(hard_limit),
                "used_space": str(used_space),
                "avail_space": str(hard_limit - used_space)
            })
        LOG.info("Get share usage:%s of dtree share:%s from storage successfully",
                 share_capacity, self.dtree_name)
        return share_capacity

    def _get_qos_param_of_namespace(self):
        """
        If cannot found qos policy of namespace on storage, return {},
        otherwise, return the actual MBPS and IOPS of this namespace
        :return: a dict of total_bytes_sec and total_iops_sec
        """
        qos_associate_param = {
            "filter": "[{\"qos_scale\": \"%s\" ,\"object_name\": \"%s\",\"account_id\": \"%s\"}]" %
                      (constants.QOS_SCALE_NAMESPACE, self.namespace_name, self.account_id)
        }
        qos_association_info = self.client.get_qos_association_info(
            qos_associate_param)
        if not qos_association_info:
            LOG.warning("Can not find associate qos policy of namespace:%s,"
                        "return {}", self.namespace_name)
            return {}

        try:
            qos_info = self.client.query_qos_info({
                "qos_scale": constants.QOS_SCALE_NAMESPACE,
                "id": qos_association_info[0].get('qos_policy_id')
            })
            if not qos_info.get('data'):
                LOG.warning("Can not find qos policy by qos_policy_id: %s,"
                            "return {}", qos_association_info[0].get('qos_policy_id'))
                return {}

            qos_param = {
                'total_bytes_sec': qos_info.get('data').get('max_mbps'),
                'total_iops_sec': qos_info.get('data').get('max_iops')
            }
            LOG.info("Query share qos policy successfully, param info is %s", qos_param)
            return qos_param
        except Exception as err:
            LOG.warning("Query qos info failed, return {}, reason is %s" % err)
            return {}
