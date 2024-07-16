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

from oslo_log import log

from ..community.community_check_update_storage import CommunityCheckUpdateStorage
from ...utils import constants, driver_utils

LOG = log.getLogger(__name__)


class SuyanSingleCheckUpdateStorage(CommunityCheckUpdateStorage):
    def __init__(self, client, share=None, driver_config=None,
                 context=None, storage_features=None):
        super(SuyanSingleCheckUpdateStorage, self).__init__(
            client, share, driver_config, context, storage_features)

    @staticmethod
    def get_impl_type():
        return constants.PLUGIN_SUYAN_SINGLE_IMPL

    @staticmethod
    def _check_and_set_tier_quota(namespace_info, all_share_usages, name_key):
        namespace_name = namespace_info.get(name_key)
        if not namespace_info.get('tier_hot_cap_limit'):
            LOG.info("Namespace %s not set hot_data_size, don't return ssd and hhd "
                     "capacity", namespace_name)
            return all_share_usages
        ssd_hard_quota = driver_utils.capacity_unit_up_conversion(
                    namespace_info.get('tier_hot_cap_limit'), constants.BASE_VALUE, 1)
        hdd_hard_quota = driver_utils.capacity_unit_up_conversion(
                    namespace_info.get('tier_cold_cap_limit'), constants.BASE_VALUE, 1)
        tier_perf_cap = json.loads(namespace_info.get('tier_perf_cap', '{}'))
        ssd_space_used = tier_perf_cap.get('hot', {}).get('used', 0)
        hdd_space_used = tier_perf_cap.get('cold', {}).get('used', 0)
        all_share_usages.get(namespace_name).update(
            {
                'ssd_hard_quota': ssd_hard_quota,
                'hdd_hard_quota': hdd_hard_quota,
                'ssd_space_used': ssd_space_used,
                'hdd_space_used': hdd_space_used
            }
        )
        return all_share_usages

    def get_all_share_usage(self):
        """苏研定制接口，获取对应帐户下所有的share信息"""
        LOG.info("begin to query all share usages")
        self._get_account_id()
        all_namespace_info = self.client.get_all_namespace_info(self.account_id)
        return self._get_all_share_usages(all_namespace_info)

    def get_pool_capabilities(self, pool_id, pool_info):
        pool_capabilities = super(SuyanSingleCheckUpdateStorage, self).get_pool_capabilities(
            pool_id, pool_info)
        # 上报硬盘池支持的分级属性
        system_capacity = self.client.query_system_capacity()
        pool_capabilities.update(self._set_tier_capacity(system_capacity, constants.POWER_BETWEEN_MB_AND_GB))
        # 上报存储热、温、冷容量
        pool_capabilities.update(self._set_support_tier_types(pool_id))
        return pool_capabilities

    def _get_all_share_usages(self, all_namespace_info):
        """
        1. 将所有的命名空间信息和其名称组成键值对
        2. 通过命名空间名称获取它所有的dtree信息
        3. 根据dtree信息获取配额信息
        """

        id_key = "id"
        name_key = "name"
        space_used_key = "space_used"
        space_hard_quota_key = "space_hard_quota"
        all_share_usages = {}
        for namespace in all_namespace_info:
            all_share_usages[namespace.get(name_key)] = {
                id_key: namespace.get(id_key),
                name_key: namespace.get(name_key),
                space_used_key: driver_utils.capacity_unit_up_conversion(
                    namespace.get(space_used_key, 0), constants.BASE_VALUE, 1),
                space_hard_quota_key: driver_utils.capacity_unit_up_conversion(
                    namespace.get(space_hard_quota_key, 0), constants.BASE_VALUE, 1)
            }
            self._check_and_set_tier_quota(namespace, all_share_usages, name_key)
            all_dtree_info = self.client.get_all_dtree_info_of_namespace(
                namespace.get(id_key))
            for dtree_info in all_dtree_info:
                dtree_quota = self.client.query_quota_by_parent(
                    dtree_info.get(id_key), constants.QUOTA_PARENT_TYPE_DTREE)
                all_share_usages[dtree_info.get(name_key)] = {
                    id_key: dtree_info.get(id_key),
                    name_key: dtree_info.get(name_key),
                    space_used_key: dtree_quota.get(space_used_key, 0.0),
                    space_hard_quota_key: dtree_quota.get(space_hard_quota_key, 0.0)
                }

        LOG.info("successfully get all share usages")
        return all_share_usages
