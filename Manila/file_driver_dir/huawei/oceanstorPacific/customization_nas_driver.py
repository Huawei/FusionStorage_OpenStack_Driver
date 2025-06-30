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

"""Huawei Nas Driver for Suyan."""
from abc import abstractmethod

from oslo_log import log

from .plugin.check_update_storage import CheckUpdateStorage
from .plugin.operate_share import OperateShare
from .plugin.share_tier import ShareTier
from .utils import constants
from .oceanstorpacific_nas import HuaweiNasDriver

LOG = log.getLogger(__name__)


class SuyanCustomizationApi(object):
    @abstractmethod
    def get_all_share_usage(self):
        pass

    @abstractmethod
    def get_share_usage(self, share, share_usages):
        pass

    @abstractmethod
    def update_qos(self, share, qos_specs):
        pass

    @abstractmethod
    def show_qos(self, share):
        pass

    @abstractmethod
    def modify_share_tier_policy(self, context, share, new_share):
        pass

    @abstractmethod
    def initialize_share_tier(self, context, share, file_path, init_type):
        pass

    @abstractmethod
    def get_share_tier_status(self, context, share):
        pass

    @abstractmethod
    def terminate_share_tier(self, context, share):
        pass

    @abstractmethod
    def _parse_cmcc_qos_options(self, share):
        pass


class HuaweiNasDriverForSuyan(HuaweiNasDriver, SuyanCustomizationApi):
    """Huawei Oceanstor Pacific Share Driver for Suyan cloud. """

    def __init__(self, *args, **kwargs):
        super(HuaweiNasDriverForSuyan, self).__init__(*args, **kwargs)
        self.querying = False
        self.plugin_factory = self.plugin_factory

    @staticmethod
    def _get_plugin_impl_type(backend_key, platform_key):
        """重新定向苏研定制化 插件路径"""
        platform_type = constants.PLATFORM_IMPL_MAPPING.get(platform_key)
        impl_type = constants.SUYAN_PRODUCT_IMPL_MAPPING.get(backend_key)
        return impl_type, platform_type

    def get_all_share_usage(self):
        """苏研定制接口，获取所有的share信息 返回存储上所有的share"""

        LOG.info("********************Do get all share usages.********************")
        if self.querying:
            return {}
        self.querying = True
        try:
            all_share_usages = self.plugin_factory.instance_service(
                CheckUpdateStorage, None, self.storage_features).get_all_share_usage()
        finally:
            self.querying = False
        return all_share_usages

    def get_share_usage(self, share, share_usages):
        """苏研定制接口，通过get_all_share_usage查询返回的所有share信息，获取到需要的share容量信息"""

        LOG.info("********************Do get share usage.********************")
        share_capacity = self.plugin_factory.instance_service(
            OperateShare, share, self.storage_features).get_share_usage(share_usages)
        return share_capacity

    def update_qos(self, share, qos_specs):
        """苏研定制接口，通过qos_specs中的qos信息，对share进行qos更新"""

        LOG.info("********************Do update qos.********************")
        self.plugin_factory.instance_service(
            OperateShare, share, self.storage_features).update_qos(qos_specs)

    def show_qos(self, share):
        """苏研定制接口，查询share所属命名空间的qos策略"""

        LOG.info("********************Do show_qos.********************")
        return self.plugin_factory.instance_service(
            OperateShare, share, self.storage_features).show_qos()

    def modify_share_tier_policy(self, context, share, new_share):
        """苏研定制接口，修改文件系统分级策略"""
        LOG.info("********************Do modify_share_tier_policy.********************")

        self.plugin_factory.instance_service(
            ShareTier, share, self.storage_features).modify_share_tier_policy(new_share)

    def initialize_share_tier(self, context, share, file_path, init_type):
        """苏研定制接口，创建共享预热/预冷任务"""
        LOG.info("********************Do initialize_share_tier.********************")
        return self.plugin_factory.instance_service(
            ShareTier, share, self.storage_features).initialize_share_tier(file_path, init_type)

    def get_share_tier_status(self, context, share):
        """苏研定制接口，查询预热/预冷任务状态"""
        LOG.info("********************Do get_share_tier_status.********************")
        return self.plugin_factory.instance_service(
            ShareTier, share).get_share_tier_status()

    def terminate_share_tier(self, context, share):
        """苏研定制接口，删除预热/预冷任务"""
        LOG.info("********************Do terminate_share_tier.********************")
        self.plugin_factory.instance_service(
            ShareTier, share, self.storage_features).terminate_share_tier()

    def _parse_cmcc_qos_options(self, share):
        """苏研定制接口，返回share冻结前的qos参数。"""

        LOG.info("********************Do parse cmcc qos options.********************")
        share_qos_info = self.plugin_factory.instance_service(
            OperateShare, share, self.storage_features).parse_cmcc_qos_options()
        return share_qos_info
