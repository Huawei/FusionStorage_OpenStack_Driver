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

from ..check_update_storage import CheckUpdateStorage
from ...utils import constants, driver_utils

LOG = log.getLogger(__name__)


class SuyanGFSCheckUpdateStorage(CheckUpdateStorage):
    def __init__(self, client, share=None, driver_config=None,
                 context=None, storage_features=None):
        super(SuyanGFSCheckUpdateStorage, self).__init__(
            client, share, driver_config, context, storage_features)

    @staticmethod
    def get_impl_type():
        return constants.PLUGIN_SUYAN_GFS_IMPL

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
            if pool_info:
                pool_capabilities = self.get_pool_capabilities(pool_info)
                data[pool_key].append(pool_capabilities)

        if data[pool_key]:
            LOG.debug(_("Updated cluster pools:{0} success".format(
                self.driver_config.pool_list)))
        else:
            err_msg = (_("Update cluster pools{0} fail.".format(self.driver_config.pool_list)))
            raise exception.InvalidInput(reason=err_msg)

    def get_pool_capabilities(self, pool_info):
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
                    pool_id=pool_info.get('id'),
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
            'support_tier_types': list(set(pool_info.get('supported_tier_types'))),
            'is_support_gfs': True,
            'cluster_pool_num': pool_info.get('storage_num')
        })

        return pool_capabilities

    def get_all_share_usage(self):
        """获取所有GFS和Dtree对应ssd容量信息+hdd信息+总容量信息"""
        name_key = 'name'
        space_used_key = 'space_used'
        LOG.info("begin to query all share usages")
        self._get_storage_pool_name()
        all_share_usages = {}
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
