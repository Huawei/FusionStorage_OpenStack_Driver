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
import os

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
        return constants.PLUGIN_SUYAN_SINGLE_IMPL, None

    @staticmethod
    def _combine_capacity_usage(all_share_usages, metrics_enum, data_infos):
        if not data_infos or len(data_infos) == 1:
            LOG.info("No data found, don't need to continue,"
                     " data_info is %s", data_infos)
            return all_share_usages

        header_line_info = data_infos[0].strip().strip('\n').split(',')
        header_line_enum = {}
        for index, metrics in enumerate(header_line_info):
            header_line_enum[index] = metrics
        for data_info in data_infos[1:]:
            data_list = data_info.strip().strip('\n').split(',')
            if len(data_list) != len(header_line_info):
                LOG.warning("Data：%s length can not match the header line:%s in file, skip",
                            data_list, header_line_info)
                continue
            object_capacity = {}
            for key, value in header_line_enum.items():
                if metrics_enum.get(value) is None:
                    continue

                # covert to capacity metrics unit form KB TO BYTE
                if value in constants.ALL_CAPACITY_METRIC_NUM:
                    object_capacity[metrics_enum.get(value)] = driver_utils.capacity_unit_up_conversion(
                        int(data_list[key]), constants.BASE_VALUE, constants.POWER_BETWEEN_BYTE_AND_KB
                    )
                else:
                    object_capacity[metrics_enum.get(value)] = data_list[key]
            all_share_usages[object_capacity.get('name')] = object_capacity
        return all_share_usages

    @staticmethod
    def _check_and_set_tier_quota(namespace_info, all_share_usages):

        tier_hot_cap_limit = namespace_info.get('tier_hot_cap_limit')
        tier_cold_cap_limit = namespace_info.get('tier_cold_cap_limit')
        if tier_hot_cap_limit is None and tier_cold_cap_limit is None:
            return all_share_usages

        ssd_hard_quota = driver_utils.capacity_unit_up_conversion(
            tier_hot_cap_limit, constants.BASE_VALUE, 1)
        hdd_hard_quota = driver_utils.capacity_unit_up_conversion(
            tier_cold_cap_limit, constants.BASE_VALUE, 1)
        tier_perf_cap = json.loads(namespace_info.get('tier_perf_cap', '{}'))

        ssd_space_used = tier_perf_cap.get('hot', {}).get(constants.USED)
        hdd_space_used = tier_perf_cap.get('cold', {}).get(constants.USED)
        all_share_usages.get(namespace_info.get(constants.NAME)).update(
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
        try:
            capacity_data = self.client.get_capacity_data_file()
            return self._get_all_share_usages_by_data_file(capacity_data)
        except Exception as err:
            LOG.info("Get all share usage from capacity data failed, reason is %s, "
                     "Try to use the batch query interface to traverse all namespaces and dtrees.", err)
            self._get_account_id()
            all_namespace_info = self.client.get_all_namespace_info(self.account_id)
            return self._get_all_share_usages_by_common(all_namespace_info)

    def get_pool_capabilities(self, pool_id, pool_info):
        pool_capabilities = super(SuyanSingleCheckUpdateStorage, self).get_pool_capabilities(
            pool_id, pool_info)
        pool_capabilities.update({
            'storage_protocol': 'NFS_CIFS_DPC',
            'share_proto': 'DPC',
            'driver_version': 1.1,
            'snapshot_support': False,
            'revert_to_snapshot_support': False
        })
        # 上报硬盘池支持的分级属性
        system_capacity = self.client.query_system_capacity()
        pool_capabilities.update(self._set_tier_capacity(system_capacity, constants.POWER_BETWEEN_MB_AND_GB))
        # 上报存储热、温、冷容量
        pool_capabilities.update(self._set_support_tier_types(pool_id))
        return pool_capabilities

    def _get_all_share_usages_by_data_file(self, capacity_data):
        all_share_usages = {}
        if not capacity_data:
            return all_share_usages

        base_dir = os.path.join('/var/tmp', constants.HUAWEI_TEMP_FILE_DIC)
        if not os.path.exists(base_dir):
            os.mkdir(base_dir, 0o750)
        self._remove_capacity_data_file(base_dir)
        try:
            self._generate_capacity_data_file(base_dir, capacity_data)
            namespace_data_infos, dtree_data_infos = self._parse_capacity_data_file(base_dir)
            self._combine_all_share_usages(all_share_usages, namespace_data_infos, dtree_data_infos)
        except Exception as err:
            LOG.error("Get all share usages failed, reason is %s", err)
            return {}
        finally:
            self._remove_capacity_data_file(base_dir)

        LOG.info("Successfully get all share usages")
        return all_share_usages

    def _combine_all_share_usages(self, all_share_usages, namespace_data_infos, dtree_data_infos):
        """
        namespace data header line exam:
        namespace_id,namespace_name,90065,90058,90059,90060,90061,90062,90063,90064
        dtree data header line exam:
        namespace_id,dtree_id,dtree_name,90065,90058,90059,90060,90061,90062,90063,90064
        """
        namespace_metrics_enmu = {
            'namespace_name': 'name',
            'namespace_id': 'id',
            '90065': 'space_used',
            '90064': 'ssd_space_used',
            '90062': 'hdd_space_used',
            '90061': 'ssd_hard_quota',
            '90059': 'hdd_hard_quota',
            '90058': 'space_hard_quota'
        }
        dtree_metrics_enum = {
            'dtree_name': 'name',
            'dtree_id': 'id',
            '90065': 'space_used',
            '90058': 'space_hard_quota'
        }
        # combine namespace capacity usage
        self._combine_capacity_usage(all_share_usages, namespace_metrics_enmu, namespace_data_infos)
        # combine dtree capacity usage
        self._combine_capacity_usage(all_share_usages, dtree_metrics_enum, dtree_data_infos)

    def _get_all_share_usages_by_common(self, all_namespace_info):
        """
        1. 将所有的命名空间信息和其名称组成键值对
        2. 通过命名空间名称获取它所有的dtree信息
        3. 根据dtree信息获取配额信息
        """

        all_share_usages = {}
        for namespace in all_namespace_info:
            all_share_usages[namespace.get(constants.NAME)] = {
                constants.ID: namespace.get(constants.ID),
                constants.NAME: namespace.get(constants.NAME),
                constants.SPACE_USED: driver_utils.capacity_unit_up_conversion(
                    namespace.get(constants.SPACE_USED, 0), constants.BASE_VALUE, 1),
                constants.SPACE_HARD_QUOTA: driver_utils.capacity_unit_up_conversion(
                    namespace.get(constants.SPACE_HARD_QUOTA, 0), constants.BASE_VALUE, 1)
            }
            self._check_and_set_tier_quota(namespace, all_share_usages)
            all_dtree_info = self.client.get_all_dtree_info_of_namespace(
                namespace.get(constants.ID))
            for dtree_info in all_dtree_info:
                dtree_quota = self.client.query_quota_by_parent(
                    dtree_info.get(constants.ID), constants.QUOTA_PARENT_TYPE_DTREE)
                all_share_usages[dtree_info.get(constants.NAME)] = {
                    constants.ID: dtree_info.get(constants.ID),
                    constants.NAME: dtree_info.get(constants.NAME),
                    constants.SPACE_USED: dtree_quota.get(constants.SPACE_USED, 0.0),
                    constants.SPACE_HARD_QUOTA: dtree_quota.get(constants.SPACE_HARD_QUOTA, 0.0)
                }
        LOG.info("Successfully get all share usages")
        return all_share_usages
