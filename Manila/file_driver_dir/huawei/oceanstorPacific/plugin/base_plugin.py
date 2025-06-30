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
import os
import stat
from abc import abstractmethod

import netaddr
from oslo_log import log
from manila import context as admin_context
from manila import exception
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
        self.enable_qos = False # 创建Qos开关
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

    @staticmethod
    def _check_share_tier_capacity_param(tier_info, total_size):
        if (tier_info.get('hot_data_size') is None or
                tier_info.get('cold_data_size') is None):
            return

        hot_data_size = int(tier_info.get('hot_data_size'))
        cold_data_size = int(tier_info.get('cold_data_size'))

        if hot_data_size > total_size or cold_data_size > total_size:
            error_msg = ("Check share tier param failed, hot_data_size:%s or "
                         "cold_data_size:%s can not bigger than share total size: %s" %
                         (hot_data_size, cold_data_size, total_size))
            LOG.error(error_msg)
            raise exception.InvalidInput(error_msg)

        if hot_data_size + cold_data_size != total_size:
            error_msg = ("Check share tier param failed, hot_data_size:%s plus "
                         "cold_data_size:%s must equal to the share total size: %s" %
                         (hot_data_size, cold_data_size, total_size))
            LOG.error(error_msg)
            raise exception.InvalidInput(error_msg)

        return

    @staticmethod
    def _check_share_tier_policy_param(tier_info):
        """
        Check share tier param is valid or not
        """
        hot_data_size = int(tier_info.get('hot_data_size', 0))
        cold_data_size = int(tier_info.get('cold_data_size', 0))
        tier_place = tier_info.get('tier_place')

        if tier_place and tier_place not in constants.SUPPORT_TIER_PLACE:
            error_msg = ("The configured tier_place:%s not in support tier place:%s, "
                         "Please Check" % (tier_place, constants.SUPPORT_TIER_PLACE))
            LOG.error(error_msg)
            raise exception.InvalidInput(error_msg)

        if hot_data_size and cold_data_size and not tier_place:
            error_msg = ("Tier place:%s must be set when hot_data_size:%s and "
                         "cold_data_size:%s all not equal to 0" %
                         (tier_place, hot_data_size, cold_data_size))
            LOG.error(error_msg)
            raise exception.InvalidInput(error_msg)

    @staticmethod
    def _set_tier_data_size(tier_info, total_size):
        """
        set hot_data_size if cold data size configured but
        hot data size not configured
        """
        hot_data_size = tier_info.get('hot_data_size')
        cold_data_size = tier_info.get('cold_data_size')
        if hot_data_size is None and cold_data_size is None:
            return tier_info
        elif hot_data_size is None:
            tier_info['hot_data_size'] = total_size - int(cold_data_size)
        elif cold_data_size is None:
            tier_info['cold_data_size'] = total_size - int(hot_data_size)

        return tier_info

    @staticmethod
    def _check_is_temp_file(base_dir, dir_info):
        current_file_abs_path = os.path.join(base_dir, dir_info)
        if not os.path.isfile(current_file_abs_path):
            return False

        if dir_info == constants.CAPACITY_DATA_FILE_NAME:
            return True
        if (dir_info.startswith(constants.NAMESPACE_DATA_FILE_PREFIX) or
                dir_info.startswith(constants.DTREE_DATA_FILE_PREFIX)):
            return True
        return False

    @staticmethod
    def _generate_capacity_data_file(base_dir, capacity_data):
        """
        Write stream data in tar file and extract tarfile under the path
        :param base_dir: file path
        :param capacity_data: stream data returned by storage restful api
        """
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        modes = stat.S_IWUSR | stat.S_IRUSR | stat.S_IRGRP
        capacity_data_zip_file_path = os.path.join(base_dir, constants.CAPACITY_DATA_FILE_NAME)
        with os.fdopen(os.open(capacity_data_zip_file_path, flags, modes), 'wb') as tar_file:
            tar_file.write(capacity_data.content)
        driver_utils.extract_zipfile(
            capacity_data_zip_file_path, base_dir,
            constants.MAX_CAPACITY_DATA_FILE_NUM, constants.MAX_CAPACITY_DATA_PER_FILE_SIZE)

    @staticmethod
    def _parse_capacity_data_file(base_dir):
        """
        Parse namespace and dtree capacity data from data file under the path
        :param base_dir: file path
        :return: namespace data list and dtree data list
        """
        capacity_data_file_lists = os.listdir(base_dir)

        for data_file in capacity_data_file_lists:
            abs_path = os.path.join(base_dir, data_file)
            if not os.path.isfile(abs_path):
                continue
            if data_file.startswith(constants.NAMESPACE_DATA_FILE_PREFIX):
                LOG.info("Begin to parse NameSpace capacity data file, file_name is %s", data_file)
                with open(abs_path, 'r') as namespace_file:
                    namespace_data_infos = namespace_file.readlines()
            elif data_file.startswith(constants.DTREE_DATA_FILE_PREFIX):
                LOG.info("Begin to parse Dtree capacity data file, file_name is %s", data_file)
                with open(abs_path, 'r') as dtree_file:
                    dtree_data_infos = dtree_file.readlines()
            else:
                continue
        return namespace_data_infos, dtree_data_infos

    @staticmethod
    def _calc_common_share_qos_mbps(qos_coefficient, share_size):
        """
        The common share uses the total share size's mbps as the final max_mbps.
        """
        share_size_to_tib = driver_utils.capacity_unit_down_conversion(
            float(share_size), constants.BASE_VALUE, constants.POWER_BETWEEN_GB_AND_TB
        )
        return driver_utils.qos_calc_formula(share_size_to_tib, qos_coefficient)

    @staticmethod
    def _get_acl_type_by_share_type(acl_policy_config):
        acl_policy = acl_policy_config.split(' ')[1]
        if not acl_policy.isdigit() or not int(acl_policy) in constants.ACL_POLICY:
            error_msg = "Acl policy must be integer and must be in %s" % constants.ACL_POLICY
            LOG.error(error_msg)
            raise exception.BadConfigurationException(error_msg)

        return int(acl_policy)

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

    def _set_qos_coefficient(self, data_size, coefficient, qos_coefficient_info):
        if data_size and not coefficient:
            error_msg = "The QoS formula matching the current resource pool type is not found," \
                        " The resource pool type is %s" % self.share_proto
            LOG.error(error_msg)
            raise exception.BadConfigurationException(error_msg)
        if data_size and coefficient:
            qos_coefficient_info[coefficient] = data_size

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
        share_tier_strategy = self.share.get('share_tier_strategy', {})
        if not isinstance(share_tier_strategy, dict):
            share_tier_strategy = {}
        share_tier_value = share_tier_strategy.get(tier_param)
        tier_value = share_tier_value if metadata_tier_value is None else metadata_tier_value
        if tier_value is not None:
            tier_info[tier_param] = tier_value

    def _get_all_share_tier_policy(self):
        """
        get all share tier policy
        :return: all tier info
        """
        tier_info = {}
        # get hot data size
        self._get_share_tier_policy(tier_info, 'hot_data_size')
        # get cold data size
        self._get_share_tier_policy(tier_info, 'cold_data_size')
        # get tier_grade
        self._get_share_tier_policy(tier_info, 'tier_place')
        # get tier_migrate_expiration
        self._get_share_tier_policy(tier_info, 'tier_migrate_expiration')

        return tier_info

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

    def _remove_capacity_data_file(self, base_dir):
        """
        Delete the residual files generated under the path.
        :param base_dir: file path
        """
        dir_info_lists = os.listdir(base_dir)
        for dir_info in dir_info_lists:
            abs_path = os.path.join(base_dir, dir_info)
            if self._check_is_temp_file(base_dir, dir_info):
                LOG.info("Temp file need to be clean up, file path is %s", abs_path)
                os.remove(abs_path)

    def _set_qos_param_by_size_and_type(self, share_size, hot_data_size=None, cold_data_size=None):
        """
        Set max_bandwidth and max_iops for common share by share_size and pool_type.
        When share is tier share, need set max_bandwidth and max_iops
        by hot_ata_size、cold data_size and pool_type.
        :param share_size: share total size
        :param hot_data_size: tier share hot data size
        :param cold_data_size: tier share cold data size
        :return: dict: qos_config of max_bandwidth and max_iops
        """
        storage_pool_id = self._get_current_storage_pool_id()
        pool_qos_param = self.driver_config.pools_type.get(
            storage_pool_id, {}).get('pool_qos_param')
        if not pool_qos_param and not self.enable_qos:
            LOG.debug("Share qos param by pool type is {}")
            return {}

        qos_config = {constants.MAX_MBPS: 0, 'max_iops': 0}
        if not pool_qos_param and self.enable_qos:
            LOG.debug("Share qos param by pool type is %s", qos_config)
            return qos_config

        qos_coefficient_info = self._get_qos_coefficient_by_protocol_and_tier(
            share_size, cold_data_size, hot_data_size, pool_qos_param)

        sum_qos_mbps = 0
        for coefficient, coefficient_value in qos_coefficient_info.items():
            sum_qos_mbps += self._calc_common_share_qos_mbps(coefficient, coefficient_value)

        qos_config[constants.MAX_MBPS] = sum_qos_mbps
        LOG.debug("Share qos param by pool type is %s", qos_config)
        return qos_config

    def _get_qos_coefficient_by_protocol_and_tier(self, share_size, cold_data_size,
                                                  hot_data_size, pool_qos_param):
        """
        Get the QoS coefficient according to the protocol and tier.
        Parameters:
            share_size
            cold_data_size
            hot_data_size
            pool_qos_param
        Return value:
        Returns a dictionary that includes the QoS coefficients and their corresponding sizes.
        """
        if hot_data_size is None and cold_data_size is None:
            if len(pool_qos_param) != 1:
                error_msg = "No tier and multi-protocol resource pools can only config one pool_type."
                LOG.error(error_msg)
                raise exception.BadConfigurationException(error_msg)
            for _, coefficient in pool_qos_param.items():
                return {coefficient: share_size}

        qos_coefficient_info = {}
        self._set_qos_coefficient(
            cold_data_size,
            pool_qos_param.get(self._get_medium_pool_type(constants.HDD_POOL_PRIORITY, 'HDD')),
            qos_coefficient_info)

        self._set_qos_coefficient(
            hot_data_size,
            pool_qos_param.get(self._get_medium_pool_type(constants.SSD_POOL_PRIORITY, 'SSD')),
            qos_coefficient_info)
        return qos_coefficient_info

    def _operate_share_qos(self, namespace_name, qos_config):
        """
        Operate share qos by qos_vals and the namespace of share's qos associate info
        :return:
        """
        qos_associate_param = {
            "filter": "[{\"qos_scale\": \"%s\" ,\"object_name\": \"%s\",\"account_id\": \"%s\"}]" %
                      (constants.QOS_SCALE_NAMESPACE, namespace_name, self.account_id)
        }
        qos_param = {
            'name': namespace_name,
            'qos_mode': constants.QOS_MODE_MANUAL,
            'qos_scale': constants.QOS_SCALE_NAMESPACE,
            'account_id': self.account_id,
            'max_mbps': int(qos_config.get('max_mbps', 0)),
            'max_iops': int(qos_config.get('max_iops', 0)),
        }
        qos_association_info = self.client.get_qos_association_info(
            qos_associate_param)
        if not qos_config and not qos_association_info:
            LOG.info("Qos param:%s not configured, the namespace of share not associate qos, "
                     "do nothing", qos_config)
        elif not qos_config and qos_association_info:
            LOG.info("Qos param:%s not configured, the namespace of share associate qos, delete"
                     "qos of namespace", qos_config)
            self.client.delete_qos(namespace_name)
        elif qos_config and not qos_association_info:
            LOG.info("Qos param:%s is configured and the namespace did not associate qos,"
                     "Create and associate qos to namespace:%s", qos_config, namespace_name)
            result = self.client.create_qos(qos_param)
            qos_policy_id = result.get('id')
            self.client.add_qos_association(namespace_name, qos_policy_id, self.account_id)
        else:
            LOG.info("Qos param:%s is configured and the namespace has already associated qos,"
                     "Update the qos info of namespace:%s", qos_config, namespace_name)
            self.client.update_qos_info(qos_param)

    def _set_share_to_share_instance(self):
        """
        When the share parameter transferred by the upper layer is the Share object,
        the value is converted to the ShareInstance object.
        """
        try:
            share_instance = self.share.get('instances')[0]
            share_instance.set_share_data(self.share)
            self.share = share_instance
        except Exception as err:
            err_msg = "Can not get share instance of share:%s" % self.share.get('id')
            LOG.error(err_msg)
            raise exception.InvalidShare(reason=err_msg)

    def _set_acl_type_policy(self):
        """
        set namespace acl_policy by share_proto and share_type
        :return:
        """
        acl_policy_config = self.share_type_extra_specs.get('acl_policy')
        if acl_policy_config is not None:
            return self._get_acl_type_by_share_type(acl_policy_config)

        if 'CIFS' not in self.share_proto:
            return constants.ACL_POLICY_UNIX

        elif len(self.share_proto) == 1:
            return constants.ACL_POLICY_NTFS

        return constants.ACL_POLICY_MIXED

    def _get_medium_pool_type(self, protocol_priority, medium):
        protocol = 'NFS'
        for priority in protocol_priority:
            if priority in self.share_proto:
                protocol = priority
                break
        return protocol + '_' + medium
