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

from abc import abstractmethod

import netaddr
from oslo_log import log
from manila import context as admin_context
from manila.share import api
from manila.share import share_types
from manila.share import utils as share_utils

from ..utils import constants, driver_utils

LOG = log.getLogger(__name__)


class BasePlugin(object):
    def __init__(self, client, share=None, driver_config=None,
                 context=None, storage_features=None):
        self.client = client
        self.share = share
        self.context = context
        self.storage_features = storage_features
        self.driver_config = driver_config
        self.account_id = None
        self.account_name = None
        self.storage_pool_id = None
        self.storage_pool_name = None
        self.share_api = api.API()
        self.share_metadata = self._get_share_metadata()
        self.share_type_extra_specs = self._get_share_type_extra_specs()
        self.share_proto = self._get_share_proto()

    @staticmethod
    @abstractmethod
    def get_impl_type():
        pass

    @staticmethod
    def standard_ipaddr(access):
        """
        When the added client permission is an IP address,
        standardize it. Otherwise, do not process it.
        """
        try:
            format_ip = netaddr.IPAddress(access)
            access_to = str(format_ip.format(dialect=netaddr.ipv6_compact))
            return access_to
        except Exception:
            return access

    @staticmethod
    def get_lowest_tier_grade(tier_types):
        if 'cold' in tier_types:
            lowest_tier_grade = 'cold'
        elif 'warm' in tier_types:
            lowest_tier_grade = 'warm'
        else:
            lowest_tier_grade = 'hot'

        return lowest_tier_grade

    @staticmethod
    def is_ipv4_address(ip_address):
        try:
            if netaddr.IPAddress(ip_address).version == 4:
                return True
            return False
        except Exception:
            return False

    def concurrent_exec_waiting_tasks(self, task_id_list):
        # Enable Concurrent Tasks and wait until all tasks complete
        threading_task_list = []
        for task_id in task_id_list:
            threading_task = driver_utils.MyThread(
                self.client.wait_task_until_complete, task_id)
            threading_task.start()
            threading_task_list.append(threading_task)
        for task in threading_task_list:
            task.get_result()

    def _get_share_metadata(self):
        try:
            share_id = self.share.get('share_id')
            if self.context is None:
                self.context = admin_context.get_admin_context()
            return self.share_api.get_share_metadata(self.context, {'id': share_id})
        except Exception:
            LOG.info("Can not get share metadata, return {}")
            return {}

    def _get_share_type_extra_specs(self):
        if self.share is None:
            return {}
        type_id = self.share.get('share_type_id')
        return share_types.get_share_type_extra_specs(type_id)

    def _get_account_id(self):
        self.account_name = self.driver_config.account_name
        result = self.client.query_account_by_name(self.account_name)
        self.account_id = result.get('id')

    def _get_share_proto(self):
        """
        Get share proto
        Priority Level: metadata > share_type > share_instance
        :return: share proto list
        """
        share_proto = []
        share_proto_key = 'share_proto'
        if self.share is None:
            return share_proto

        metadata_share_proto = self.share_metadata.get(share_proto_key, '')
        if metadata_share_proto:
            return metadata_share_proto.split(constants.MULTI_PROTO_SEPARATOR)

        type_share_proto = self.share_type_extra_specs.get(share_proto_key, '').split(
            constants.MULTI_PROTO_SEPARATOR)
        if 'DPC' in type_share_proto:
            share_proto.append('DPC')
            return share_proto

        return self.share.get(share_proto_key, '').split(constants.MULTI_PROTO_SEPARATOR)

    def _get_share_parent_id(self):
        """
        Get share parent_share_id
        Priority Level: metadata > share_instance
        :return: share parent_share_id
        """
        metadata_parent_share_id = self.share_metadata.get('parent_share_id')
        if not metadata_parent_share_id:
            return self.share.get('parent_share_id')
        return metadata_parent_share_id

    def _get_share_tier_policy(self, tier_info, tier_param):
        """
        get tier policy
        Priority Level: metadata > share_instance
        :param tier_info: all tier info dict
        :param tier_param: tier policy key
        :return:
        """
        metadata_tier_value = self.share_metadata.get(tier_param)
        share_tier_value = self.share.get('share_tier_strategy', {}).get(tier_param)
        tier_value = metadata_tier_value or share_tier_value
        if tier_value:
            tier_info[tier_param] = tier_value

    def _get_all_share_tier_policy(self):
        """
        get all share tier policy
        :return: all tier info
        """
        tier_info = {}
        # get hot data size
        self._get_share_tier_policy(tier_info, 'hot_data_size')
        # get tier_grade
        self._get_share_tier_policy(tier_info, 'tier_place')
        # get tier_migrate_expiration
        self._get_share_tier_policy(tier_info, 'tier_migrate_expiration')
        return tier_info

    def _get_share_access_proto(self):
        """
        1.Multi proto:Get access proto from metadata or share instance
        from key access_proto
        Priority Level: metadata > share_instance
        2.Single proto: Get access proto from share type or share instance
        from key share_proto
        Priority Level: share type > share_instance
        :return:
        """
        metadata_access_proto = self.share_metadata.get('access_proto')
        instance_access_proto = self.share.get('access_proto')

        access_proto = metadata_access_proto or instance_access_proto
        if access_proto:
            return [access_proto]

        return self.share_proto

    def _get_forbidden_dpc_param(self):
        if 'DPC' in self.share_proto:
            return constants.NOT_FORBIDDEN_DPC
        return constants.FORBIDDEN_DPC

    def _get_current_storage_pool_id(self):
        self._get_storage_pool_name()
        return self.storage_features.get(self.storage_pool_name).get('pool_id')

    def _get_storage_pool_name(self):
        self.storage_pool_name = share_utils.extract_host(
            self.share.get('host'), level='pool')

    def _is_tier_scenarios(self):
        """
        Check is this backend a tier scenarios backend,
        Tier scenarios: Customer configured two disk type enum
        :return: Boolean
        """
        tier_scenarios_tuple = (
            self.driver_config.hot_disk_type and self.driver_config.warm_disk_type,
            self.driver_config.hot_disk_type and self.driver_config.cold_disk_type,
            self.driver_config.cold_disk_type and self.driver_config.warm_disk_type,
        )
        if any(tier_scenarios_tuple):
            return True
        return False
