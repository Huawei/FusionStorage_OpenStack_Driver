# coding=utf-8
# Copyright (c) 2026 Huawei Technologies Co., Ltd.
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
from concurrent.futures import ThreadPoolExecutor

from oslo_log import log
from manila import exception
from manila.i18n import _
from ..community.community_check_update_storage import CommunityCheckUpdateStorage
from ...utils import constants, driver_utils

LOG = log.getLogger(__name__)


class DmeCheckUpdateStorage(CommunityCheckUpdateStorage):
    def __init__(self, client, share=None, driver_config=None, context=None, storage_features=None):
        super(DmeCheckUpdateStorage, self).__init__(client, share, driver_config, context, storage_features)

    @staticmethod
    def get_impl_type():
        return constants.PLUGIN_DME_FILESYSTEM_IMPL, None

    @staticmethod
    def _get_share_id_by_info_name(info_name):
        if not info_name.startswith('share-'):
            return ''

        return info_name.split('share-')[1]

    def get_all_share_usage(self):
        all_share_usages = {}
        param = self._build_query_param()

        if param.get('a800') is not None:
            self._set_a800_share_usage(param, all_share_usages)
        if param.get('pacific') is not None:
            self._set_pacific_share_usage(param, all_share_usages)
        LOG.info("All share usages is %s" % all_share_usages)
        return all_share_usages

    def check_service(self):
        pass

    def update_storage_pool(self, data):

        pool_capabilities = dict(
            qos=True,
            reserved_percentage=int(self.driver_config.reserved_percentage),
            reserved_share_extend_percentage=int(self.driver_config.reserved_percentage),
            max_over_subscription_ratio=float(self.driver_config.max_over_ratio),
            ipv6_support=True,
            dedupe=False,
            thin_provisioning=True,
            compression=True,
            snapshot_support=[False, False],
            create_share_from_snapshot_support=[False, False],
            revert_to_snapshot_support=[False, False],
            acl_policy=constants.ACL_POLICY,
            storage_protocol='NFS_DPC',
            share_proto='DPC'
        )

        self._update_storage_pool_capabilities(pool_capabilities)
        data.update({'pools': [pool_capabilities]})

    def _update_storage_pool_capabilities(self, pool_capabilities):
        a800_pool_capabilities, pacific_pool_capabilities = {}, {}
        pool_name_list, pool_id_list = [], []
        if self.driver_config.A800:
            a800_pool_capabilities = self._get_pool_capabilities({
                    "storage_id": self.driver_config.A800.storage_id,
                    "raw_id": self.driver_config.A800.pool_raw_id,
                    "zone_id": self.driver_config.A800.zone_id})
            pool_name_list.append(a800_pool_capabilities.get('pool_name'))
            pool_id_list.append(a800_pool_capabilities.get('pool_id'))
        if self.driver_config.Pacific:
            pacific_pool_capabilities = self._get_pool_capabilities({
                    "storage_id": self.driver_config.Pacific.storage_id,
                    "raw_id": self.driver_config.Pacific.pool_raw_id})
            pool_name_list.append(pacific_pool_capabilities.get('pool_name'))
            pool_id_list.append(pacific_pool_capabilities.get('pool_id'))
        pool_capabilities.update({
            'total_capacity_gb': a800_pool_capabilities.get('total_capacity_gb', 0) +
                                 pacific_pool_capabilities.get('total_capacity_gb', 0),
            'free_capacity_gb': a800_pool_capabilities.get('free_capacity_gb', 0) +
                                 pacific_pool_capabilities.get('free_capacity_gb', 0),
            'hot_total_capacity_gb': a800_pool_capabilities.get('total_capacity_gb', 0),
            'hot_free_capacity_gb': a800_pool_capabilities.get('free_capacity_gb', 0),
            'cold_total_capacity_gb': pacific_pool_capabilities.get('total_capacity_gb', 0),
            'cold_free_capacity_gb': pacific_pool_capabilities.get('free_capacity_gb', 0),
            'support_tier_types': ['hot', 'cold'],
            'pool_name': ','.join(pool_name_list),
            'pool_id': ','.join(pool_id_list)
        })

    def _get_pool_capabilities(self, param):
        pool = self.client.query_specified_pool(param)
        total_capacity_gb = float(pool.get('total_capacity', 0)) / constants.BASE_VALUE
        if pool.get('free_capacity') is not None:
            free_capacity_gb = float(pool.get('free_capacity', 0)) / constants.BASE_VALUE
        else:
            free_capacity = pool.get('total_capacity', 0) - pool.get('consumed_capacity', 0)
            free_capacity_gb = float(free_capacity) / constants.BASE_VALUE
        return {
            'total_capacity_gb': round(total_capacity_gb, 2),
            'free_capacity_gb': round(free_capacity_gb, 2),
            'pool_name': pool.get('name'),
            'pool_id': str(pool.get('id'))
        }

    def _set_a800_share_usage(self, param, all_share_usages):
        with ThreadPoolExecutor(max_workers=2) as executor:
            future_file_system = executor.submit(self.client.get_file_systems, param.get('a800'))
            future_fs_dtree = executor.submit(
                self._get_dtrees_and_quotas, param.get('a800'), self.driver_config.A800.vstore_id)

            file_systems = future_file_system.result()
            fs_dtrees, fs_dtree_quotas = future_fs_dtree.result()

        self._set_file_system_usage(file_systems, all_share_usages)
        quotas_index = {quota.get('parent_raw_id'): quota for quota in fs_dtree_quotas}
        self._set_dtree_usage(fs_dtrees, quotas_index, all_share_usages)

    def _set_pacific_share_usage(self, param, all_share_usages):
        with ThreadPoolExecutor(max_workers=2) as executor:
            future_name_space = executor.submit(self.client.query_namespaces, param.get('pacific'))
            future_fs_dtree = executor.submit(self._get_dtrees_and_quotas, param.get('pacific'))

            name_spaces = future_name_space.result()
            ns_dtrees, ns_dtree_quotas = future_fs_dtree.result()

        self._set_name_space_usage(name_spaces, all_share_usages)
        quotas_index = {quota.get('parent_raw_id'): quota for quota in ns_dtree_quotas}
        self._set_dtree_usage(ns_dtrees, quotas_index, all_share_usages)


    def _set_file_system_usage(self, file_systems, all_share_usages):
        for file_system in file_systems:
            name = file_system.get('name')
            share_id = self._get_share_id_by_info_name(name)
            if not share_id:
                LOG.debug("The filesystem %s is not created from manila, don't need to return", name)
                continue

            hard_limit = file_system.get('total_capacity_in_byte', 0)
            avail_space = file_system.get('available_capacity_in_byte', 0)
            used_space = hard_limit - avail_space

            all_share_usages[share_id] = {
                'used_space': str(int(used_space)),
                'avail_space': str(int(avail_space)),
                'hard_limit': str(int(hard_limit)),
                'ssd_hard_limit': str(int(hard_limit)),
                'ssd_used_space': str(int(used_space)),
                'ssd_avail_space': str(int(avail_space)),
                'hdd_hard_limit': 0,
                'hdd_used_space': 0,
                'hdd_avail_space': 0
            }

    def _set_name_space_usage(self, name_spaces, all_share_usages):
        for name_space in name_spaces:
            name = name_space.get('name')
            share_id = self._get_share_id_by_info_name(name)
            if not share_id:
                LOG.debug("The namespace %s is not created from manila, don't need to return", name)
                continue
            try:
                hard_limit = round(float(name_space.get('space_hard_quota', 0)) * constants.BASE_VALUE, 2)
                used_space = round(float(name_space.get('space_used', 0)) * constants.BASE_VALUE, 2)
            except TypeError as err:
                LOG.warning("The namespace %s don't set space_hard_quota, escape", name)
                continue
            avail_space = hard_limit - used_space

            all_share_usages[share_id] = {
                'used_space': str(int(used_space)),
                'avail_space': str(int(avail_space)),
                'hard_limit': str(int(hard_limit)),
                'hdd_hard_limit': str(int(hard_limit)),
                'hdd_used_space': str(int(used_space)),
                'hdd_avail_space': str(int(avail_space)),
                'ssd_hard_limit': 0,
                'ssd_used_space': 0,
                'ssd_avail_space': 0
            }

    def _set_dtree_usage(self, dtrees, quotas_index, all_share_usages):
        for dtree in dtrees:
            name = dtree.get('name')
            share_id = self._get_share_id_by_info_name(name)
            if not share_id:
                LOG.debug("The dtree %s is not created from manila, don't need to return", name)
                continue

            quota = quotas_index.get(dtree.get('id_in_storage'), {})
            hard_limit = quota.get('space_hard_quota', 0)
            used_space = quota.get('space_used', 0)
            avail_space = hard_limit - used_space

            all_share_usages[share_id] = {
                'used_space': str(int(used_space)),
                'avail_space': str(int(avail_space)),
                'hard_limit': str(int(hard_limit)),
            }

    def _get_dtrees_and_quotas(self, param, vstore_id=None):
        dtrees = self.client.get_dtrees(param)
        if vstore_id is not None:
            dtrees = [dtree for dtree in dtrees if dtree.get('vstore_id') == vstore_id]
        else:
            dtrees = [dtree for dtree in dtrees]
        quotas = self._get_dtree_directory_quotas(param)

        return dtrees, quotas

    def _get_dtree_directory_quotas(self, param):
        quota_param = {
            'storage_id': param.get('storage_id'),
            'quota_type': 'directory_quota',
            'parent_type': 'qtree'
        }
        if 'zone_id' in param:
            quota_param['zone_id'] = param.get('zone_id')
        return self.client.get_quotas(quota_param)

    def _build_query_param(self):
        query_param = {}
        if self.driver_config.A800:
            zone_id = self.driver_config.A800.zone_id
            if not self.driver_config.A800.zone_id:
                zone_id = self.driver_config.A800.storage_id
            query_param['a800'] = {
                'storage_id': self.driver_config.A800.storage_id,
                'zone_id': zone_id,
                'vstore_raw_id': self.driver_config.A800.vstore_raw_id,
                'name': 'share-'
            }
        if self.driver_config.Pacific:
            query_param['pacific'] = {
                'storage_id': self.driver_config.Pacific.storage_id,
                'vstore_raw_id': self.driver_config.Pacific.vstore_raw_id,
                'name': 'share-'
            }
        return query_param