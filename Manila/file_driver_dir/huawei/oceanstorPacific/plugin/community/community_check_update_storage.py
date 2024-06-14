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


class CommunityCheckUpdateStorage(CheckUpdateStorage):
    def __init__(self, client, share=None, driver_config=None,
                 context=None, storage_features=None):
        super(CommunityCheckUpdateStorage, self).__init__(
            client, share, driver_config, context, storage_features)

    @staticmethod
    def get_impl_type():
        return constants.PLUGIN_COMMUNITY_IMPL

    @staticmethod
    def _set_storage_pool_capacity(pool_info):
        """
        report storage pool total、free、provisioned capacity
        :param pool_info: storagepool, info
        :return:
        """
        storagepool_capacity = {
            'total_capacity_gb': round(driver_utils.capacity_unit_down_conversion(
                float(pool_info.get('totalCapacity')),
                constants.BASE_VALUE, constants.POWER_BETWEEN_MB_AND_GB), 1),
            'free_capacity_gb': round(driver_utils.capacity_unit_down_conversion(
                float(pool_info.get('totalCapacity')) - float(pool_info.get('usedCapacity')),
                constants.BASE_VALUE, constants.POWER_BETWEEN_MB_AND_GB), 2)
        }
        return storagepool_capacity

    def check_service(self):
        for pool_id in self.driver_config.pool_list:
            result = self.client.query_pool_info(pool_id)[0]
            status_code = result['status']

            if status_code in constants.POOL_STATUS_OK:
                LOG.info(_("The storage pool(id:{0}) is healthy.".format(pool_id)))
            else:
                err_msg = _("The storage pool(id:{0}) is unhealthy.".format(pool_id))
                raise exception.InvalidHost(reason=err_msg)

        LOG.info(_('All the storage pools are healthy.'))

    def update_storage_pool(self, data):
        """
        更新所有存储池的能力和容量信息
        :param data:
        :return: 所有存储池的能力和容量信息
        """
        pool_key = 'pools'
        data[pool_key] = []
        all_pool_info = self.client.query_pool_info()

        # check the number of storage pool
        if len(all_pool_info) > 1 or len(all_pool_info) == 0:
            err_msg = ("update storage pools failed, "
                       "the storage pool num of cluster must be 1")
            LOG.error(err_msg)
            raise exception.InvalidInput(reason=err_msg)

        pool_info = all_pool_info[0]
        for pool_id in self.driver_config.pool_list:
            if pool_info.get('storagePoolId') == pool_id:
                pool_capabilities = self.get_pool_capabilities(pool_id, pool_info)
                data[pool_key].append(pool_capabilities)

        if data[pool_key]:
            LOG.debug(_("Updated storage pools:{0} success".format(self.driver_config.pool_list)))
        else:
            err_msg = (_("Update storage pools{0} fail.".format(self.driver_config.pool_list)))
            raise exception.InvalidInput(reason=err_msg)

    def get_pool_capabilities(self, pool_id, pool_info):
        """
        获取单个存储池的容量和支持能力信息
        :param pool_id: 存储池ID
        :param pool_info: 存储池信息
        :return:
        """
        pool_capabilities = dict(
                    pool_name=pool_info.get('storagePoolName'),
                    qos=True,
                    reserved_percentage=self.driver_config.reserved_percentage,
                    max_over_subscription_ratio=self.driver_config.max_over_ratio,
                    ipv6_support=True,
                    share_proto='DPC',
                    pool_id=pool_id
                )
        # 上报存储池容量信息
        pool_capabilities.update(self._set_storage_pool_capacity(pool_info))
        return pool_capabilities

    def get_all_share_usage(self):
        pass

    def _set_tier_capacity(self):
        """
        report system ssd,sata,sas total、free、used capacity
        :return:
        """
        system_capacity = self.client.query_system_capacity()
        total_capacity_enum = {
            'ssd': 'ssd_total_capacity_converged',
            'sas': 'sas_total_capacity_converged',
            'sata': 'sata_total_capacity_converged',
        }
        used_capacity_enum = {
            'ssd': 'ssd_used_capacity_converged',
            'sas': 'sas_used_capacity_converged',
            'sata': 'sata_used_capacity_converged',
        }
        hot_total_capacity = float(system_capacity.get(total_capacity_enum.get(
            self.driver_config.hot_disk_type)))
        warm_total_capacity = float(system_capacity.get(total_capacity_enum.get(
            self.driver_config.warm_disk_type)))
        cold_total_capacity = float(system_capacity.get(total_capacity_enum.get(
            self.driver_config.cold_disk_type)))
        hot_used_capacity = float(system_capacity.get(used_capacity_enum.get(
            self.driver_config.hot_disk_type)))
        warm_used_capacity = float(system_capacity.get(used_capacity_enum.get(
            self.driver_config.warm_disk_type)))
        cold_used_capacity = float(system_capacity.get(used_capacity_enum.get(
            self.driver_config.cold_disk_type)))
        tier_capacity = {
            'hot_total_capacity_gb': round(driver_utils.capacity_unit_down_conversion(
                hot_total_capacity, constants.BASE_VALUE, constants.POWER_BETWEEN_MB_AND_GB), 1),
            'hot_free_capacity_gb': round(driver_utils.capacity_unit_down_conversion(
                hot_total_capacity - hot_used_capacity,
                constants.BASE_VALUE, constants.POWER_BETWEEN_MB_AND_GB), 2),
            'warm_total_capacity_gb': round(driver_utils.capacity_unit_down_conversion(
                warm_total_capacity, constants.BASE_VALUE, constants.POWER_BETWEEN_MB_AND_GB), 1),
            'warm_free_capacity_gb': round(driver_utils.capacity_unit_down_conversion(
                warm_total_capacity - warm_used_capacity,
                constants.BASE_VALUE, constants.POWER_BETWEEN_MB_AND_GB), 2),
            'cold_total_capacity_gb': round(driver_utils.capacity_unit_down_conversion(
                cold_total_capacity, constants.BASE_VALUE, constants.POWER_BETWEEN_MB_AND_GB), 1),
            'cold_free_capacity_gb': round(driver_utils.capacity_unit_down_conversion(
                cold_total_capacity - cold_used_capacity,
                constants.BASE_VALUE, constants.POWER_BETWEEN_MB_AND_GB), 2)
        }
        return tier_capacity

    def _set_support_tier_types(self, pool_id):
        """
        report disk pool tier type list
        :param pool_id: storage pool id
        :return:
        """
        support_tier_types = set()
        disk_pool_info_list = self.client.query_disk_pool_by_storagepool_id(pool_id)
        for disk_pool in disk_pool_info_list:
            pool_tier_type = str(disk_pool.get('poolTier', ''))
            if pool_tier_type and pool_tier_type in constants.DISK_POOL_TIER_ENUM:
                support_tier_types.add(constants.DISK_POOL_TIER_ENUM.get(pool_tier_type))
        return {'support_tier_types': list(support_tier_types)}
