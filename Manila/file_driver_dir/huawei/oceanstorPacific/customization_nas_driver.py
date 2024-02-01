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

"""Huawei Nas Driver for Suyan."""

from oslo_log import log

from .oceanstorpacific_nas import HuaweiNasDriver

LOG = log.getLogger(__name__)
SUYAN_NAS_DRIVER = ("manila.share.drivers.huawei.oceanstorPacific.customization_connection"
                    ".OceanStorPacificStorageConnectionForSuyan")


class HuaweiNasDriverForSuyan(HuaweiNasDriver):
    """Huawei Oceanstor Pacific Share Driver for Suyan cloud. """

    @staticmethod
    def _get_backend_driver_class(backend_key=None):
        """重新定向苏研定制化 插件路径"""
        return SUYAN_NAS_DRIVER

    def get_all_share_usage(self):
        """苏研定制接口，获取所有的share信息 返回存储上所有的share"""

        LOG.info("********************Do get all share usages.********************")
        all_share_usages = self.plugin.get_all_share_usage()
        return all_share_usages

    def get_share_usage(self, share, share_usages):
        """苏研定制接口，通过get_all_share_usage查询返回的所有share信息，获取到需要的share容量信息"""

        LOG.info("********************Do get share usage.********************")
        share_capacity = self.plugin.get_share_usage(share, share_usages)
        return share_capacity

    def update_qos(self, share, qos_specs):
        """苏研定制接口，通过qos_specs中的qos信息，对share进行qos更新"""

        LOG.info("********************Do update qos.********************")
        self.plugin.update_qos(share, qos_specs)

    def _parse_cmcc_qos_options(self, share):
        """苏研定制接口，返回share绑定的qos信息"""

        LOG.info("********************Do parse cmcc qos options.********************")
        share_qos_info = self.plugin.parse_cmcc_qos_options(share)
        return share_qos_info
