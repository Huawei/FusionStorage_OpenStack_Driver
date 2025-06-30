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

from oslo_log import log

from manila import exception
from manila.i18n import _

from ..community.community_check_update_storage import CommunityCheckUpdateStorage
from ...utils import constants, driver_utils

LOG = log.getLogger(__name__)


class SuyanGFSCheckUpdateStorage(CommunityCheckUpdateStorage):
    def __init__(self, client, share=None, driver_config=None,
                 context=None, storage_features=None):
        super(SuyanGFSCheckUpdateStorage, self).__init__(
            client, share, driver_config, context, storage_features)

    @staticmethod
    def get_impl_type():
        return constants.PLUGIN_SUYAN_GFS_IMPL, None

    @staticmethod
    def _get_tier_capacity(size, size_unit):
        if not size_unit:
            size_unit = constants.CAP_KB
        return driver_utils.convert_capacity(float(size), size_unit, constants.CAP_BYTE)

    def check_service(self):
        pass

    def update_storage_pool(self, data):
        """
        update gfs cluster statistics data of capacity and capabilities
        :param data:
        :return: dict of pool capabilities
        """
        pool_key = 'pools'
        data[pool_key] = []

        for pool in self.driver_config.pool_list:
            pool_info = self.client.query_cluster_statistics_by_name(pool)
            pool_id = pool_info.get('id')
            if pool_id:
                pool_capabilities = self.get_pool_capabilities(pool_id, pool_info)
                data[pool_key].append(pool_capabilities)

        if data[pool_key]:
            LOG.debug(_("Updated cluster pools:{0} success".format(
                self.driver_config.pool_list)))
        else:
            err_msg = (_("Update cluster pools{0} fail.".format(self.driver_config.pool_list)))
            raise exception.InvalidInput(reason=err_msg)

    def get_pool_capabilities(self, pool_id, pool_info):
        """
        get cluster capacity and capabilities
        :param pool_info: cluster statistics info
        :return:
        """
        total = round(driver_utils.capacity_unit_down_conversion(
            pool_info.get('total_capacity'), constants.BASE_VALUE,
            constants.POWER_BETWEEN_BYTE_AND_GB), constants.DEFAULT_VALID_BITS)
        free = round(driver_utils.capacity_unit_down_conversion(
            pool_info.get('free_capacity'), constants.BASE_VALUE,
            constants.POWER_BETWEEN_BYTE_AND_GB), constants.DEFAULT_VALID_BITS)
        # report capacity
        pool_capabilities = dict(
                    pool_name=pool_info.get('name'),
                    pool_id=pool_id,
                    qos=True,
                    free_capacity_gb=free,
                    total_capacity_gb=total,
                    reserved_percentage=self.driver_config.reserved_percentage,
                    max_over_subscription_ratio=self.driver_config.max_over_ratio,
                    ipv6_support=True,
                    share_proto='DPC',
                )
        # report gfs capabilities and tier types
        pool_capabilities.update({
            'support_tier_types': list(set(pool_info.get('supported_tiers'))),
            'is_support_gfs': True,
            'cluster_pool_num': pool_info.get('storage_num')
        })
        # 上报存储热、温、冷容量
        pool_capabilities.update(self._set_tier_capacity(pool_info, constants.POWER_BETWEEN_BYTE_AND_GB))

        return pool_capabilities

    def get_all_share_usage(self):
        """获取所有GFS和Dtree对应ssd容量信息+hdd信息+总容量信息"""
        name_key = 'name'
        space_used_key = 'space_used'
        LOG.info("begin to query all share usages")
        all_share_usages = {}
        for pool in self.driver_config.pool_list:
            self.storage_pool_name = pool
            gfs_capacities_infos = self.client.get_all_gfs_capacities_info(self.storage_pool_name)
            dtrees_capacities_infos = self.client.get_all_gfs_dtree_capacities_info(self.storage_pool_name)
            for gfs_capacity in gfs_capacities_infos:
                gfs_name = gfs_capacity.get(name_key)
                quota = gfs_capacity.get('quota').get('directory_quota', {})
                unit_type = quota.get('unit_type', constants.CAP_KB)
                all_share_usages[gfs_name] = {
                    space_used_key: self._get_tier_capacity(quota.get(space_used_key, 0), unit_type),
                    'space_hard_quota': self._get_tier_capacity(quota.get('hard_quota', 0), unit_type)
                }
                self._check_and_set_tier_quota(gfs_capacity, all_share_usages, name_key)

            for dtree_capacities in dtrees_capacities_infos:
                dtree_name = dtree_capacities.get(name_key)
                quota = dtree_capacities.get('quota').get('directory_quota', {})
                unit_type = quota.get('unit_type', constants.CAP_KB)
                all_share_usages[dtree_name] = {
                    space_used_key: self._get_tier_capacity(quota.get(space_used_key, 0), unit_type),
                    'space_hard_quota': self._get_tier_capacity(quota.get('hard_quota', 0), unit_type),
                }

        LOG.info("successfully get all share usages")
        return all_share_usages

    def _check_and_set_tier_quota(self, gfs_info, all_share_usages, name_key):
        """判断gfs是否设置ssd和hhd，如果设置则获取ssd和hhd的容量信息"""
        gfs_name = gfs_info.get(name_key)
        disk_pool_limit = gfs_info.get('disk_pool_limit', {})
        if not disk_pool_limit:
            LOG.info("Gfs %s not set hot_data_size, don't return ssd and hhd "
                     "capacity", gfs_name)
            return all_share_usages
        ssd_hard_quota = self._get_tier_capacity(disk_pool_limit.get('tier_hot_limit', 0), constants.CAP_KB)
        hdd_hard_quota = self._get_tier_capacity(disk_pool_limit.get('tier_cold_limit', 0), constants.CAP_KB)
        ssd_space_used = self._get_tier_capacity(disk_pool_limit.get('tier_hot_used', 0), constants.CAP_KB)
        hdd_space_used = self._get_tier_capacity(disk_pool_limit.get('tier_cold_used', 0), constants.CAP_KB)
        all_share_usages.get(gfs_name).update(
            {
                'ssd_hard_quota': ssd_hard_quota,
                'hdd_hard_quota': hdd_hard_quota,
                'ssd_space_used': ssd_space_used,
                'hdd_space_used': hdd_space_used
            }
        )
        return all_share_usages

    def _set_tier_capacity(self, system_capacity, unit_power):
        """
        report system ssd,sata,sas total、free、used capacity
        :return:
        """
        hot_total_capacity = float(system_capacity.get(constants.DME_TOTAL_CAPACITY_ENUM.get(
            self.driver_config.hot_disk_type, constants.DME_SSD_TOTAL_CAP_KEY), 0))
        warm_total_capacity = float(system_capacity.get(constants.DME_TOTAL_CAPACITY_ENUM.get(
            self.driver_config.warm_disk_type, constants.DME_SAS_TOTAL_CAP_KEY), 0))
        cold_total_capacity = float(system_capacity.get(constants.DME_TOTAL_CAPACITY_ENUM.get(
            self.driver_config.cold_disk_type, constants.DME_SATA_TOTAL_CAP_KEY), 0))
        hot_used_capacity = float(system_capacity.get(constants.DME_USED_CAPACITY_ENUM.get(
            self.driver_config.hot_disk_type, constants.DME_SSD_USED_CAP_KEY), 0))
        warm_used_capacity = float(system_capacity.get(constants.DME_USED_CAPACITY_ENUM.get(
            self.driver_config.warm_disk_type, constants.DME_SAS_USED_CAP_KEY), 0))
        cold_used_capacity = float(system_capacity.get(constants.DME_USED_CAPACITY_ENUM.get(
            self.driver_config.cold_disk_type, constants.DME_SATA_USED_CAP_KEY), 0))
        tier_capacity = {
            'hot_total_capacity_gb': round(driver_utils.capacity_unit_down_conversion(
                hot_total_capacity, constants.BASE_VALUE, unit_power), 1),
            'hot_free_capacity_gb': round(driver_utils.capacity_unit_down_conversion(
                hot_total_capacity - hot_used_capacity,
                constants.BASE_VALUE, unit_power), 2),
            'warm_total_capacity_gb': round(driver_utils.capacity_unit_down_conversion(
                warm_total_capacity, constants.BASE_VALUE, unit_power), 1),
            'warm_free_capacity_gb': round(driver_utils.capacity_unit_down_conversion(
                warm_total_capacity - warm_used_capacity,
                constants.BASE_VALUE, unit_power), 2),
            'cold_total_capacity_gb': round(driver_utils.capacity_unit_down_conversion(
                cold_total_capacity, constants.BASE_VALUE, unit_power), 1),
            'cold_free_capacity_gb': round(driver_utils.capacity_unit_down_conversion(
                cold_total_capacity - cold_used_capacity,
                constants.BASE_VALUE, unit_power), 2)
        }
        return tier_capacity
