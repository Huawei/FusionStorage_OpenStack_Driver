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

        with ThreadPoolExecutor(max_workers=2) as executor:
            future_file_system = executor.submit(self.client.get_file_systems, param)
            future_dtree = executor.submit(self._get_dtrees_and_quotas, param)

            file_systems = future_file_system.result()
            dtrees, dtree_quotas = future_dtree.result()

        self._set_file_system_usage(file_systems, all_share_usages)
        quotas_index = {quota.get('parent_raw_id'): quota for quota in dtree_quotas}
        self._set_dtree_usage(dtrees, quotas_index, all_share_usages)

        return all_share_usages

    def check_service(self):
        pass

    def update_storage_pool(self, data):
        param = {
            "storage_id": self.driver_config.storage_id,
            "raw_id": self.driver_config.pool_raw_id,
            "zone_id": self.driver_config.zone_id
        }
        pool = self.client.query_specified_pool(param)
        pool_capabilities = dict(
            pool_name=pool.get('name'),
            pool_id=pool.get('raw_id'),
            sn=self.driver_config.storage_sn,
            qos=True,
            reserved_percentage=int(self.driver_config.reserved_percentage),
            reserved_share_extend_percentage=int(self.driver_config.reserved_percentage),
            max_over_subscription_ratio=float(self.driver_config.max_over_ratio),
            total_capacity_gb=pool.get("total_capacity"),
            free_capacity_gb=pool.get("free_capacity"),
            ipv6_support=True,
            dedupe=False,
            thin_provisioning=True,
            compression=True,
            snapshot_support=[True, False],
            create_share_from_snapshot_support=[False, False],
            revert_to_snapshot_support=[True, False],
            acl_policy=constants.ACL_POLICY)

        data.update({'pools': [pool_capabilities]})


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

    def _get_dtrees_and_quotas(self, param):
        dtrees = self.client.get_dtrees(param)
        vstore_id = self.driver_config.vstore_id
        dtrees = [dtree for dtree in dtrees if dtree.get('vstore_id') == vstore_id]
        quotas = self._get_dtree_directory_quotas()

        return dtrees, quotas

    def _get_dtree_directory_quotas(self):
        param = {
            'storage_id': self.driver_config.storage_id,
            'zone_id': self.driver_config.zone_id,
            'quota_type': 'directory_quota',
            'parent_type': 'qtree'
        }

        return self.client.get_quotas(param)

    def _build_query_param(self):
        return {
            'storage_id': self.driver_config.storage_id,
            'zone_id': self.driver_config.zone_id if self.driver_config.zone_id else self.driver_config.storage_id,
            'vstore_raw_id': self.driver_config.vstore_raw_id,
            'name': 'share-'
        }