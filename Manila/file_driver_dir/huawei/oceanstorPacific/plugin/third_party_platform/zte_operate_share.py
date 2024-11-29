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

from ..operate_share import OperateShare
from ...utils import constants, driver_utils

LOG = log.getLogger(__name__)


class ZTEOperateShare(OperateShare):

    def __init__(self, client, share=None, driver_config=None,
                 context=None, storage_features=None):
        super(ZTEOperateShare, self).__init__(
            client, share, driver_config, context, storage_features)
        self.namespace_name = None  # 命名空间名称
        self.namespace_id = None  # 命名空间Id
        self.export_locations = None  # share路径信息
        self.qos_param = {}

    @staticmethod
    def get_impl_type():
        return constants.PLUGIN_COMMUNITY_IMPL, constants.PLUGIN_ZTE_PLATFORM_IMPL

    def reload_qos(self, qos_vals):
        """
        ZTE Platform Customization
        Implement creates, updates, and deletes share QoS.
        :param qos_vals:qos param which need to be update
        :return:
        """
        if (not self.share.get('export_locations') or not self.share.get(
                'export_locations')[0].get('path')):
            err_msg = "Ensure share fail for invalid export location."
            LOG.error(err_msg)
            raise exception.InvalidShare(reason=err_msg)

        self._check_and_get_namespace_info()
        self._parse_qos_params(qos_vals)
        self._operate_share_qos(self.namespace_name, self.qos_param)
        LOG.info("Reload qos of namespace:%s successfully", self.namespace_name)

    def _check_and_get_namespace_info(self):
        """
        1.Get namespace name from share export locations
        2.query qos info from storage by namespace name
        :return: namespace info on storage
        """
        self._get_account_id()
        self.export_locations = self.share.get('export_locations')[0].get('path')
        self.namespace_name = self.export_locations.split('\\')[-1].split('/')[-1]
        namespace_info = self.client.query_namespace_by_name(self.namespace_name)
        if not namespace_info:
            error_msg = "Reload share qos failed, Can not find the namespace of share"
            LOG.error(error_msg)
            raise exception.InvalidShare(error_msg)
        self.namespace_id = namespace_info.get('id')
        return namespace_info

    def _parse_qos_params(self, qos_vals):
        """
        Check whether the qos parameter is valid and
        parse the qos parameter to an object that
        can be identified by the storage device.
        :param qos_vals:qos param which need to be check and parse
        :return:
        """
        if not qos_vals:
            return

        self._set_qos_param(qos_vals)
        self._check_qos_mandatory_param(qos_vals)

    def _set_qos_param(self, qos_vals):
        """
        check qos param whether in valid qos param list
        set qos maxIOPS and maxMBPS param by qos_vals:
        if both maxIOPS and total_iops_sec is configured, the maxIOPS priority is greater than total_iops_sec
        if both maxMBPS and total_bytes_sec is configured, the maxMBPS priority is greater than total_bytes_sec
        :param qos_vals: qos param need to be check and parse
        :return:
        """
        iops_key = 'max_iops'
        mbps_key = 'max_mbps'
        for qos_key, qos_value in qos_vals.items():
            if qos_key not in constants.QOS_KEYS:
                error_msg = ("The qos param:%s of qos_vals:%s is not valid, Only support "
                             "these params:%s") % (qos_key, qos_vals, constants.QOS_KEYS)
                LOG.error(error_msg)
                raise exception.InvalidInput(error_msg)

            if qos_key == "maxIOPS":
                self.qos_param[iops_key] = int(math.ceil(qos_value))
            elif qos_key == "total_iops_sec" and self.qos_param.get(iops_key) is None:
                self.qos_param[iops_key] = int(math.ceil(qos_value))
            elif qos_key == "maxMBPS":
                self.qos_param[mbps_key] = int(math.ceil(qos_value))
            elif qos_key == "total_bytes_sec" and self.qos_param.get(mbps_key) is None:
                qos_value = int(qos_value)
                self.qos_param[mbps_key] = int(math.ceil(driver_utils.capacity_unit_down_conversion(
                        float(qos_value), constants.BASE_VALUE,
                        constants.POWER_BETWEEN_BYTE_AND_MB)))

    def _check_qos_mandatory_param(self, qos_vals):
        for qos_key in constants.QOS_MUST_SET:
            if qos_key not in self.qos_param:
                msg = ('The qos param of qos_vals:%s is invalid, one of [maxIOPS, total_iops_sec] must be set,'
                       'one of [maxMBPS, total_bytes_sec] must be set' % qos_vals)
                LOG.error(msg)
                raise exception.InvalidInput(reason=msg)
