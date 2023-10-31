# coding=utf-8
# Copyright (c) 2023 Huawei Technologies Co., Ltd.
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

from oslo_log import log

from manila import exception
from manila.i18n import _

from .operate_share import OperateShare
from .change_access import ChangeAccess
from .check_update_storage import CheckUpdateStorage
from ..helper import constants

LOG = log.getLogger(__name__)


class CustomizationOperate(OperateShare):

    def set_root(self, root):
        self.root = root
        return self

    def get_share_usage(self, share_usages):
        """苏研定制接口，通过share_usages获取对应share的容量信息"""

        self._get_namespace_name_from_export_locations()
        share_capacity = {}

        for share_data in share_usages:
            if share_data.get("name") != self.namespace_name:
                continue
            if share_data.get("space_hard_quota") is None or share_data.get("space_used") is None:
                err_msg = _("Can not get share data, the share data is {0}".format(share_data))
                LOG.error(err_msg)
                raise exception.InvalidShare(reason=err_msg)

            share_capacity = {
                "hard_limit": str(share_data.get("space_hard_quota")),
                "used_space": str(share_data.get("space_used")),
                "avail_space": str(share_data.get("space_hard_quota") - share_data.get("space_used"))
            }
            return share_capacity
        return share_capacity

    def _get_max_band_width_qos_config(self, extra_specs):
        """
        苏研单独的qos 参数设置与读取，其支持的参数如下：
             “total_bytes_sec”：总吞吐量，单位Byte/s
             “total_iops_sec”： 总IOPS，单位个/s
        此处解析 max_band_width，从total_bytes_sec获取
        """
        # the total_bytes_sec is Byte/s the pacific need MB/s
        tmp_max_band_width = extra_specs.get('pacific:total_bytes_sec')
        if tmp_max_band_width is None:
            self.qos_config['max_band_width'] = constants.MAX_BAND_WIDTH
        elif (tmp_max_band_width.strip().isdigit()
                and 1 <= int(int(tmp_max_band_width.strip()) / constants.BYTE_TO_MB)
                      <= constants.BAND_WIDTH_UPPER_LIMIT):
            self.qos_config['max_band_width'] = int(int(tmp_max_band_width.strip()) / constants.BYTE_TO_MB)
        else:
            err_msg = _("The total_bytes_sec in share type "
                        "must be int([1, %s]).") % constants.BAND_WIDTH_UPPER_LIMIT
            raise exception.InvalidInput(reason=err_msg)

    def _get_max_iops_qos_config(self, extra_specs):
        """
        苏研单独的qos 参数设置与读取，其支持的参数如下：
             “total_bytes_sec”：总吞吐量，单位Byte/s
             “total_iops_sec”： 总IOPS，单位个/s
        此处解析 max_iops，从total_iops_sec获取
        """
        tmp_max_iops = extra_specs.get('pacific:total_iops_sec')
        if tmp_max_iops is None:
            self.qos_config['max_iops'] = constants.MAX_IOPS
        elif tmp_max_iops.strip().isdigit() \
                and 0 <= int(tmp_max_iops.strip()) <= constants.MAX_IOPS_UPPER_LIMIT:
            self.qos_config['max_iops'] = int(tmp_max_iops.strip())
        else:
            err_msg = _("The max_iops in share type "
                        "must be int([0, %s]).") % constants.MAX_IOPS_UPPER_LIMIT
            raise exception.InvalidInput(reason=err_msg)

    def _create_qos(self):
        qos_name = self.namespace_name
        try:
            result = self.helper.create_qos_for_suyan(qos_name, self.account_id, self.qos_config)
            qos_policy_id = result.get('id')
            self.helper.add_qos_association(self.namespace_name, qos_policy_id, self.account_id)
        except Exception as e:
            self._rollback_creat(2)
            raise e

    def _get_namespace_name_from_export_locations(self):
        if not self.share['export_locations'] or not self.share['export_locations'][0]['path']:
            err_msg = _("Get namespace_name fail for invalid export location.")
            LOG.error(err_msg)
            raise exception.InvalidShare(reason=err_msg)

        export_locations = self.share['export_locations'][0]['path']
        self.namespace_name = export_locations.split('\\')[-1].split('/')[-1]

    def _find_account_name(self, root=None):
        LOG.info("Get account name from xml.")
        if not root:
            err_msg = _("Can not get account name from config.")
            LOG.error(err_msg)
            raise exception.InvalidShare(reason=err_msg)
        account_name = root.findtext('Filesystem/AccountName').strip()
        if not account_name:
            err_msg = "Can not get account_name from xml, please check."
            LOG.error(err_msg)
            raise exception.InvalidShare(reason=err_msg)
        return account_name

    def _get_update_qos_config(self, qos_specs):
        tmp_max_band_width = str(qos_specs.get('total_bytes_sec'))
        if (tmp_max_band_width.strip().isdigit()
                and 1 <= int(int(tmp_max_band_width.strip()) / constants.BYTE_TO_MB)
                      <= constants.BAND_WIDTH_UPPER_LIMIT):
            self.qos_config['max_band_width'] = int(int(tmp_max_band_width.strip()) / constants.BYTE_TO_MB)

        tmp_max_iops = str(qos_specs.get('total_iops_sec'))
        if tmp_max_iops.strip().isdigit() \
                and 0 <= int(tmp_max_iops.strip()) <= constants.MAX_IOPS_UPPER_LIMIT:
            self.qos_config['max_iops'] = int(tmp_max_iops.strip())

    def _create_qos_when_update_qos(self, qos_name):
        try:
            result = self.helper.create_qos_for_suyan(qos_name, self.account_id, self.qos_config)
            qos_policy_id = result.get('id')
            self.helper.add_qos_association(self.namespace_name, qos_policy_id, self.account_id)
        except Exception as e:
            self.helper.delete_qos(self.namespace_name)
            raise e

    def _update_qos(self, qos_name):
        self.helper.change_qos_for_suyan(qos_name, self.account_id, self.qos_config)

    def update_qos(self, qos_specs, root):
        """苏研定制接口，根据传递的qos_specs，刷新share的qos信息，如果没有则创建对应qos"""

        self._get_update_qos_config(qos_specs)
        if not self.qos_config:
            err_msg = "Can not get qos config when update_qos, the qos_specs is {0}".format(qos_specs)
            LOG.error(err_msg)
            raise exception.InvalidShare(reason=err_msg)

        account_name = self._find_account_name(root)
        result = self.helper.query_account_by_name(account_name)
        self.account_id = result.get('id')
        self._get_namespace_name_from_export_locations()

        qos_name = self.namespace_name
        qos_info = self.helper.query_qos_info_by_name(qos_name)
        if not qos_info.get("data"):
            self._create_qos_when_update_qos(qos_name)
        else:
            self._update_qos(qos_name)

    def parse_cmcc_qos_options(self):
        """苏研定制接口，查询share相关的qos 信息"""

        self._get_namespace_name_from_export_locations()
        result = self.helper.query_qos_info_by_name(self.namespace_name)
        if not result.get("data"):
            return {}
        qos_info = result.get("data", {})

        share_qos_info = {
            "total_bytes_sec": qos_info.get("max_mbps", 0) * constants.BYTE_TO_MB,
            "total_iops_sec": qos_info.get("max_iops", 0)
        }
        return share_qos_info


class CustomizationChangeAccess(ChangeAccess):
    def __init__(self, helper, share, root):
        super(CustomizationChangeAccess, self).__init__(helper, share)
        self.root = root

    def _find_account_id(self):
        LOG.info("Find account id from xml, call by ChangeAccess")
        account_name = self.root.findtext('Filesystem/AccountName').strip()
        result = self.helper.query_account_by_name(account_name)
        self.account_id = result.get('id')


class CustomizationChangeCheckUpdateStorage(CheckUpdateStorage):
    def get_all_share_usage(self):
        """苏研定制接口，获取对应帐户下所有的share信息"""

        account_name = self.root.findtext('Filesystem/AccountName').strip()
        result = self.helper.query_account_by_name(account_name)
        account_id = result.get('id')
        all_share_usage = self.helper.get_all_namespace_info(account_id)
        return all_share_usage
