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

    @staticmethod
    def get_impl_type():
        return constants.PLUGIN_SUYAN_SINGLE_IMPL

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
        if not self.share.get('export_locations')[0]:
            err_msg = _("update share qos fail for invalid export location.")
            raise exception.InvalidShare(reason=err_msg)

        self._get_update_qos_config(qos_specs)

        self._get_account_id()
        self._get_namespace_name_for_qos()

        qos_name = self.namespace_name
        qos_info = self.client.query_qos_info_by_name(qos_name)
        if not qos_info.get("data"):
            self._create_qos_when_update_qos(qos_name)
        else:
            self._update_qos(qos_name)

    def parse_cmcc_qos_options(self):
        """苏研定制接口，解冻前需要先获取要恢复的qos信息"""
        share_qos_info = {
            "total_bytes_sec": 0,
            "total_iops_sec": 0
        }
        return share_qos_info

    def get_share_usage(self, share_usages):
        """苏研定制接口，通过share_usages获取对应share的容量信息"""

        if (not self.share.get('export_locations') or not self.share.get(
                'export_locations')[0].get('path')):
            err_msg = _("Get namespace_name fail for invalid export location.")
            LOG.error(err_msg)
            raise exception.InvalidShare(reason=err_msg)

        self._get_parent_name_from_export_locations()

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

    def _get_max_band_width_qos_config(self, extra_specs):
        """
        苏研单独的qos 参数设置与读取，其支持的参数如下：
             “total_bytes_sec”：总吞吐量，单位Byte/s
             “total_iops_sec”： 总IOPS，单位个/s
        此处解析 max_band_width，从total_bytes_sec获取
        临时方案先统一将qos设置为无限制
        """
        # the total_bytes_sec is Byte/s the pacific need MB/s
        self.qos_config['max_band_width'] = constants.QOS_UNLIMITED

    def _get_max_iops_qos_config(self, extra_specs):
        """
        苏研单独的qos 参数设置与读取，其支持的参数如下：
             “total_bytes_sec”：总吞吐量，单位Byte/s
             “total_iops_sec”： 总IOPS，单位个/s
        此处解析 max_iops，从total_iops_sec获取
        临时方案先统一将qos设置为无限制
        """
        self.qos_config['max_iops'] = constants.QOS_UNLIMITED

    def _create_qos(self):
        qos_name = self.namespace_name
        try:
            result = self.client.create_qos_for_suyan(qos_name, self.account_id, self.qos_config)
            qos_policy_id = result.get('id')
            self.client.add_qos_association(self.namespace_name, qos_policy_id, self.account_id)
        except Exception as e:
            self._rollback_creat(2)
            raise e

    def _get_parent_name_from_export_locations(self):
        """二级目录场景下获取namespace名称的方式有差异"""

        export_location = self.share.get('export_locations')[0].get('path')
        self._get_namespace_name_from_location(export_location)

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

    def _create_qos_when_update_qos(self, qos_name):
        try:
            result = self.client.create_qos_for_suyan(qos_name, self.account_id, self.qos_config)
            qos_policy_id = result.get('id')
            self.client.add_qos_association(self.namespace_name, qos_policy_id, self.account_id)
        except Exception as e:
            self.client.delete_qos(self.namespace_name)
            raise e

    def _update_qos(self, qos_name):
        self.client.change_qos_for_suyan(qos_name, self.account_id, self.qos_config)

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
        if self.share_parent_id:
            share_info = share_usages.get(self.dtree_name)
        else:
            share_info = share_usages.get(self.namespace_name)

        if not share_info:
            return {}

        return self._check_and_get_share_capacity(share_info)

    def _get_namespace_name_for_qos(self):
        """
        the share param of update_qos and parse_cmcc_qos_options
        is different from other interface
        """
        export_location = self.share.get('export_locations')[0]
        self._get_namespace_name_from_location(export_location)

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
