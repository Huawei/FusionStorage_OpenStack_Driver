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

from ...utils import constants

from ..community.community_operate_share import CommunityOperateShare
from ...utils import driver_utils

LOG = log.getLogger(__name__)


class SuyanGFSOperateShare(CommunityOperateShare):
    def __init__(self, client, share=None, driver_config=None,
                 context=None, storage_features=None):
        super(SuyanGFSOperateShare, self).__init__(
            client, share, driver_config, context, storage_features)
        self.share_parent_id = self._get_share_parent_id()
        self.gfs_name_locator = None
        self.gfs_param = {}
        self.gfs_dtree_param = {}

    @staticmethod
    def get_impl_type():
        return constants.PLUGIN_SUYAN_GFS_IMPL

    @staticmethod
    def _set_quota_param(name_locator, new_size):
        new_size_in_kb = driver_utils.convert_capacity(
            new_size, constants.CAP_GB, constants.CAP_KB)
        modify_param = {
            'name_locator': name_locator,
            'quota': {
                'directory_quota': {
                    'space_quota': {
                        'hard_quota': new_size_in_kb,
                        'unit_type': constants.CAP_KB
                    }
                }
            }
        }
        return modify_param

    def create_share(self):
        if not self.share_parent_id:
            return self.create_gfs()

        return self.create_gfs_dtree()

    def delete_share(self):
        if (not self.share.get('export_locations') or not self.share.get(
                'export_locations')[0].get('path')):
            LOG.warning("Delete share fail for invalid export location.")
            return False

        if not self.share_parent_id:
            return self.delete_gfs()

        return self.delete_gfs_dtree()

    def ensure_share(self):
        if (not self.share.get('export_locations') or not self.share.get(
                'export_locations')[0].get('path')):
            err_msg = "Ensure share fail for invalid export location."
            raise exception.InvalidShare(reason=err_msg)

        gfs_infos = self._get_gfs_info()
        self._check_gfs_status(gfs_infos)
        return self._get_ensure_share_location()

    def create_gfs(self):
        """
        create gfs and return the gfs mount path
        :return: list of gfs location path
        """
        self._set_gfs_create_param()
        self._create_gfs()
        self._create_gfs_smart_features()
        return self._get_gfs_location()

    def create_gfs_dtree(self):
        """
        create gfs dtree and return the gfs mount path
        :return: list of gfs location path
        """
        self._set_gfs_dtree_create_param()
        self._create_gfs_dtree()
        return self._get_gfs_location()

    def delete_gfs(self):
        self._get_storage_pool_name()
        self.namespace_name = 'share-' + self.share.get('share_id')
        gfs_delete_param = {
            'name_locator': self.namespace_name + '@' + self.storage_pool_name
        }

        # if gfs not exist, no need to query task, delete success
        try:
            result = self.client.delete_gfs(gfs_delete_param)
        except exception.ShareNotFound:
            LOG.warning('GFS not exist, no need to continue')
            return False

        try:
            self.client.wait_task_until_complete(result.get('task_id'))
        except Exception as err:
            LOG.error("Delete GFS task failed, reason is %s", err)
            raise err

        return True

    def delete_gfs_dtree(self):
        name_locator_list = []
        self._get_storage_pool_name()
        name_locator_list.append('share-' + self.share.get('share_id'))
        name_locator_list.append('share-' + self.share_parent_id)
        name_locator_list.append(self.storage_pool_name)
        gfs_dtree_delete_param = {
            'name_locator': '@'.join(name_locator_list)
        }

        # if gfs or dtree not exist, no need to query task, delete success
        try:
            result = self.client.delete_gfs_dtree(gfs_dtree_delete_param)
        except exception.ShareNotFound:
            LOG.warning('GFS or GFS_Dtree not exist, no need to continue')
            return False

        try:
            self.client.wait_task_until_complete(result.get('task_id'))
        except Exception as err:
            LOG.error("Delete GFS Dtree task failed, reason is %s", err)
            raise err

        return True

    def change_share(self, new_size, action):
        self._get_storage_pool_name()
        if not self.share.get('export_locations') or not self.share.get('export_locations')[0].get('path'):
            err_msg = _("change share fail for invalid export location.")
            raise exception.InvalidShare(reason=err_msg)

        cluster_name = self.storage_pool_name
        task_id_key = 'task_id'
        if not self.share_parent_id:
            # gfs场景
            new_hot_size = self._get_all_share_tier_policy().get('hot_data_size')
            gfs_name = constants.SHARE_PREFIX + self.share.get('share_id')
            name_locator = '@'.join([gfs_name, cluster_name])
            # 修改GFS分级容量
            gfs_tier_cap_modify_result = self._check_and_update_gfs_tier_size(
                name_locator, new_size, new_hot_size)
            if gfs_tier_cap_modify_result:
                self.client.wait_task_until_complete(gfs_tier_cap_modify_result.get(task_id_key))
            # 修改GFS配额容量
            gfs_quota_modify_result = self._update_gfs_quota_size(name_locator, new_size)
            self.client.wait_task_until_complete(gfs_quota_modify_result.get(task_id_key))
        else:
            # dtree场景
            gfs_name = constants.SHARE_PREFIX + self.share_parent_id
            dtree_name = constants.SHARE_PREFIX + self.share.get('share_id')
            name_locator = '@'.join([dtree_name, gfs_name, cluster_name])
            self._check_space_for_dtree(name_locator, new_size)
            modify_param = self._set_quota_param(name_locator, new_size)
            result = self.client.change_gfs_dtree_size(modify_param)
            self.client.wait_task_until_complete(result.get(task_id_key))

        LOG.info("{0} share done. New size:{1}.".format(action, new_size))
        return True

    def get_share_usage(self, share_usages):
        """获取单个GFS和Dtree对应ssd容量信息+hdd信息+总容量信息"""
        if not self.share_parent_id:
            share_name = 'share-' + self.share.get('share_id')
        else:
            share_name = 'share-' + self.share_parent_id

        share_info = share_usages.get(share_name)
        if not share_info:
            return {}

        return self._check_and_get_share_capacity(share_info)

    def update_qos(self, qos_specs):
        """
        根据传递的qos_specs，刷新share的qos信息，
        如果没有则创建对应qos, 此接口的share不是share_instance对象是share对象
        """
        LOG.info("Begin to update gfs qos")
        if not self.share.get('export_locations')[0]:
            err_msg = _("update share qos fail for invalid export location.")
            raise exception.InvalidShare(reason=err_msg)
        self._get_update_qos_config(qos_specs)
        self.namespace_name = constants.SHARE_PREFIX + self.share.get('id')
        self._get_storage_pool_name()
        qos_query_param = {
            'gfs_name_locator': self.namespace_name + "@" + self.storage_pool_name
        }
        update_and_create_qos_param = {
            'gfs_name_locator': self.namespace_name + "@" + self.storage_pool_name,
            'qos_list': [{
                'name': self.namespace_name,
                'max_ops': self.qos_config.get('max_iops'),
                'max_mbps': self.qos_config.get('max_band_width')
            }]
        }
        qos_info = self.client.query_gfs_qos_policy(qos_query_param)
        if qos_info:
            result = self.client.update_gfs_qos_policy(update_and_create_qos_param)
            try:
                self.client.wait_task_until_complete(result.get('task_id'))
            except Exception as err:
                LOG.error("Update GFS qos task failed, reason is %s", err)
                raise err
        else:
            result = self.client.create_gfs_qos_policy(update_and_create_qos_param)
            try:
                self.client.wait_task_until_complete(result.get('task_id'))
            except Exception as err:
                LOG.error("Create GFS qos task failed, reason is %s", err)
                raise err
        LOG.info("Success to update gfs qos")

    def _check_and_update_gfs_tier_size(self, name_locator, new_hard_size, new_hot_size):
        gfs_detail = self.client.query_gfs_detail(name_locator)
        org_hard_size_in_gb = self._get_quota_in_gb(gfs_detail)

        if new_hard_size <= org_hard_size_in_gb:
            err_msg = ("not allowed to shrinkage, new_hard_size: %s, "
                       "org_hard_size_in_gb: %s") % (new_hard_size, org_hard_size_in_gb)
            LOG.info(err_msg)
            raise exception.InvalidShare(reason=err_msg)

        if new_hot_size is None:
            return {}

        # 冷、热、总都不能缩
        new_hot_size = int(new_hot_size)
        org_hot_size_in_gb = self._get_tier_limit(gfs_detail, 'tier_hot_limit')
        org_cold_size_in_gb = self._get_tier_limit(gfs_detail, 'tier_cold_limit')

        new_cold_size = new_hard_size - new_hot_size
        if new_cold_size < org_cold_size_in_gb:
            err_msg = ("not allowed to shrink size, new_cold_size: %s, "
                       "org_cold_size_in_gb: %s") % (new_cold_size, org_cold_size_in_gb)
            LOG.info(err_msg)
            raise exception.InvalidShare(reason=err_msg)

        if new_hot_size < org_hot_size_in_gb:
            err_msg = ("not allowed to shrinkage, new_hot_size: %s, "
                       "org_hot_size_in_gb: %s") % (new_hot_size, org_hot_size_in_gb)
            LOG.info(err_msg)
            raise exception.InvalidShare(reason=err_msg)

        new_size_in_kb = driver_utils.convert_capacity(
            new_hard_size, constants.CAP_GB, constants.CAP_KB)
        new_hot_size_in_kb = driver_utils.convert_capacity(
            new_hot_size, constants.CAP_GB, constants.CAP_KB)
        modify_param = {
            "name_locator": name_locator,
            "disk_pool_limit": {
                "tier_hot_limit": new_hot_size_in_kb,
                "tier_cold_limit": new_size_in_kb - new_hot_size_in_kb,
                "unit_type": constants.CAP_KB
            }
        }
        result = self.client.change_gfs_size(modify_param)
        return result

    def _update_gfs_quota_size(self, name_locator, new_size):
        modify_param = self._set_quota_param(name_locator, new_size)
        return self.client.change_gfs_quota_size(modify_param)

    def _check_space_for_dtree(self, name_locator, new_hard_size_in_gb):
        dtree_detail = self.client.query_gfs_dtree_detail(name_locator)
        org_hard_size_in_gb = self._get_quota_in_gb(dtree_detail)

        if new_hard_size_in_gb < org_hard_size_in_gb:
            err_msg = _("not allowed to shrinkage, new_hard_size: {0}, org_hard_size: {1}")
            LOG.info(err_msg)
            raise exception.InvalidShare(reason=err_msg)

    def _get_quota_in_gb(self, resp):
        size = resp.get('quota', {}).get('directory_quota', {}).get('hard_quota', 0)
        size_unit = resp.get('quota', {}).get('directory_quota', {}).get('unit_type')
        if not size_unit:
            size_unit = constants.CAP_KB
        return driver_utils.convert_capacity(size, size_unit, constants.CAP_GB)

    def _get_tier_limit(self, resp, level):
        size = resp.get('disk_pool_limit', {}).get(level, 0)
        size_unit = resp.get('disk_pool_limit', {}).get('unit_type')
        if not size_unit:
            size_unit = constants.CAP_KB
        return driver_utils.convert_capacity(size, size_unit, constants.CAP_GB)

    def _create_gfs(self):
        """
        Calling DME create gfs restful url and wait task complete
        :return:
        """
        result = self.client.create_gfs(self.gfs_param)
        try:
            self.client.wait_task_until_complete(result.get('task_id'))
        except Exception as err:
            LOG.error("Create GFS task failed, reason is %s", err)
            raise err

    def _create_gfs_smart_features(self):
        """
        In this interface, we will go to create gfs tier
        policy and qos policy
        we will Deliver tasks in a unified
        manner and wait until all tasks are complete.
        :return:
        """
        self.gfs_name_locator = '@'.join([self.namespace_name, self.storage_pool_name])
        gfs_tier_grade_param, gfs_tier_migrate_param = self._check_and_get_tier_param()
        qos_param = {
            'gfs_name_locator': self.gfs_name_locator,
            'qos_list': [{
                'name': self.namespace_name,
                'max_ops': constants.QOS_UNLIMITED,
                'max_mbps': constants.QOS_UNLIMITED
            }]
        }
        gfs_delete_param = {
            'name_locator': self.gfs_name_locator
        }
        # Deliver all tasks
        try:
            task_id_list = self._deliver_gfs_smart_features_task(
                qos_param, gfs_tier_grade_param, gfs_tier_migrate_param
            )
        except Exception as err:
            LOG.error("task create failed, do rollback, reason is %s", err)
            self.client.delete_gfs(gfs_delete_param)
            raise err

        # Enable Concurrent Tasks and wait until all tasks complete
        try:
            self.concurrent_exec_waiting_tasks(task_id_list)
        except Exception as err:
            LOG.error("task execute failed, do rollback, reason is %s", err)
            self.client.delete_gfs(gfs_delete_param)
            raise err

    def _get_tier_grade_param(self, tier_grade_param):
        """
        set tier grade create param of gfs
        :param tier_grade_param: empty dict to be update
        :return: final tier grade create dict param
        """
        tier_grade = self.tier_info.get('tier_place')
        if not tier_grade:
            LOG.info("Tier place policy didn't configure, Don't need create")
            return tier_grade_param

        tier_grade_param.update({
            'gfs_name_locator': self.gfs_name_locator,
            'name': self.namespace_name + constants.GRADE_NAME,
            'tier_grade': tier_grade
        })
        return tier_grade_param

    def _get_tier_migrate_param(self, tier_migrate_param, current_tier_types):
        """
        get tier migrate create param
        :param tier_migrate_param: empty dict
        :param current_tier_types: storage tier type list
        :return: final tier param dict
        """
        tier_migrate_expiration = self.tier_info.get('tier_migrate_expiration')
        if not tier_migrate_expiration:
            LOG.info("Tier tier migrate expiration didn't configure, Don't need create")
            return tier_migrate_param

        tier_migrate_param.update({
            'gfs_name_locator': self.gfs_name_locator,
            'name': self.namespace_name + constants.PERIODICITY_NAME,
            'migration_type': constants.DME_MIGRATE_PERIODIC,
            'tier_grade': self.get_lowest_tier_grade(current_tier_types),
            'atime_filter': {
                'atime_operator': constants.DME_ATIME_RATHER_THAN,
                'atime': int(tier_migrate_expiration),
                'atime_unit': constants.HTIME_UNIT
            }
        })
        return tier_migrate_param

    def _deliver_gfs_smart_features_task(
            self, qos_param, gfs_tier_grade_param, gfs_tier_migrate_param):
        """
        deliver all tasks and return task_id_list
        :param qos_param: the param of create qos
        :param gfs_tier_grade_param: the param of create tier grade
        :param gfs_tier_migrate_param: the param of create tier migrate
        :return: all create task id list
        """
        task_id_list = []
        task_id_key = 'task_id'
        LOG.info("Begin to create gfs qos policy")
        qos_task_result = self.client.create_gfs_qos_policy(qos_param)
        task_id_list.append(qos_task_result.get(task_id_key))
        if gfs_tier_grade_param:
            LOG.info("Begin to create gfs tier grade policy")
            tier_grade_result = self.client.create_gfs_tier_grade_policy(
                gfs_tier_grade_param)
            task_id_list.append(tier_grade_result.get(task_id_key))
        if gfs_tier_migrate_param:
            LOG.info("Begin to create gfs tier migreate policy")
            tier_migrate_task_result = self.client.create_gfs_tier_migration_policy(
                gfs_tier_migrate_param)
            task_id_list.append(tier_migrate_task_result.get(task_id_key))
        return task_id_list

    def _create_gfs_dtree(self):
        result = self.client.create_gfs_dtree(self.gfs_dtree_param)
        try:
            self.client.wait_task_until_complete(result.get('task_id'))
        except Exception as err:
            LOG.error("Create GFS Dtree task failed, reason is %s", err)
            raise err

    def _get_gfs_location(self):
        """
        return gfs mount path
        :return:
        """
        location = []
        if 'DPC' in self.share_proto:
            dpc_path = '/' + self.namespace_name
            location.append('DPC:' + dpc_path)

        return location

    def _set_gfs_create_param(self):
        self._get_storage_pool_name()
        self.namespace_name = 'share-' + self.share.get('share_id')
        gfs_param = {
            'cluster_classification_name': self.storage_pool_name,
            'name': self.namespace_name,
            'scattered_num': self._get_scattered_value('scattered_num'),
            'scattered_level': self._get_scattered_value('scattered_level'),
            'quota': {
                'directory_quota': {
                    'space_quota': {
                        'hard_quota': driver_utils.capacity_unit_up_conversion(
                            self.share.get('size'), constants.BASE_VALUE,
                            constants.POWER_BETWEEN_KB_AND_GB),
                        'unit_type': constants.DME_DEFAULT_CAPACITY_UNIT
                    }
                }
            }
        }
        # set tier data capacity limit
        disk_pool_size_limit_param = self._set_disk_pool_size_limit_param()
        if disk_pool_size_limit_param:
            gfs_param['disk_pool_limit'] = disk_pool_size_limit_param
        self.gfs_param = gfs_param

    def _set_disk_pool_size_limit_param(self):
        """
        set tier data size if hot_data_size exist
        :return:
        """
        disk_pool_size_limit_param = {}
        self.tier_info = self._get_all_share_tier_policy()
        hot_data_size = self.tier_info.get('hot_data_size')
        total_size = self.share.get('size')
        if hot_data_size is None:
            return disk_pool_size_limit_param
        hot_data_size = int(hot_data_size)
        if hot_data_size > total_size:
            LOG.warning("the configured hot data size %s is bigger than total size, "
                        "set it to total siz %s", hot_data_size, total_size)
            hot_data_size = total_size
        disk_pool_size_limit_param.update({
            'tier_hot_limit': str(driver_utils.capacity_unit_up_conversion(
                hot_data_size, constants.BASE_VALUE, constants.POWER_BETWEEN_KB_AND_GB)),
            'tier_cold_limit': str(driver_utils.capacity_unit_up_conversion(
                total_size - hot_data_size,
                constants.BASE_VALUE, constants.POWER_BETWEEN_KB_AND_GB
            )),
            'unit_type': constants.DME_DEFAULT_CAPACITY_UNIT
        })
        return disk_pool_size_limit_param

    def _set_gfs_dtree_create_param(self):
        self._get_storage_pool_name()
        self.namespace_name = 'share-' + self.share_parent_id
        self.gfs_dtree_param = {
            'gfs_name_locator': self.namespace_name + '@' + self.storage_pool_name,
            'dtree_name': 'share-' + self.share.get('share_id'),
            'quota': {
                'directory_quota': {
                    'space_quota': {
                        'hard_quota': driver_utils.capacity_unit_up_conversion(
                            self.share.get('size'), constants.BASE_VALUE,
                            constants.POWER_BETWEEN_KB_AND_GB),
                        'unit_type': constants.DME_DEFAULT_CAPACITY_UNIT
                    }
                }
            }
        }

    def _get_scattered_value(self, scattered_key):
        """
        get scattered num, Priority Level: metadata > share_instance
        :return:
        """
        metadata_scattered = self.share_metadata.get(scattered_key)
        if metadata_scattered is not None:
            scattered_value = metadata_scattered
        else:
            scattered_value = self.share.get(scattered_key)

        # check is scattered_num param an integer or not
        if isinstance(scattered_value, int):
            return scattered_value
        if not scattered_value.isdigit():
            raise exception.InvalidInput("{0}:{1} must be int".format(scattered_key, scattered_value))
        return int(scattered_value)

    def _get_gfs_info(self):
        self._get_storage_pool_name()
        self._set_namespace_name()
        query_gfs_param = {
            'name': self.namespace_name,
            'cluster_classification_name': self.storage_pool_name
        }
        return self.client.get_gfs_info_by_name(query_gfs_param)

    def _set_namespace_name(self):
        if self.share_parent_id:
            self.namespace_name = 'share-' + self.share_parent_id
        else:
            self.namespace_name = 'share-' + self.share.get('share_id')

    def _check_gfs_status(self, gfs_infos):
        gfs_status = ''
        for gfs_info in gfs_infos:
            gfs_status = gfs_info.get('running_status', '')
            break

        if gfs_status != constants.GFS_RUNNING_STATUS_NORMAL:
            err_msg = ("The running status of gfs ({0}) is not normal.".format(
                self.namespace_name))
            raise exception.InvalidShare(reason=err_msg)
