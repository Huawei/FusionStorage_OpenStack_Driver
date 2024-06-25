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
        self.gfs_param = {}
        self.gfs_dtree_param = {}

    @staticmethod
    def get_impl_type():
        return constants.PLUGIN_SUYAN_GFS_IMPL

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
            'name_locator': self.storage_pool_name + '@' + self.namespace_name
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
        name_locator_list.append(self.storage_pool_name)
        name_locator_list.append('share-' + self.share_parent_id)
        name_locator_list.append('share-' + self.share.get('share_id'))

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
        if not self.share_parent_id:
            # gfs场景
            new_hot_size = self._get_all_share_tier_policy().get('hot_data_size')
            gfs_name = constants.SHARE_PREFIX + self.share.get('share_id')
            name_locator = '@'.join([cluster_name, gfs_name])
            self._check_space_for_gfs(name_locator, new_size, new_hot_size)
            result = self.client.change_gfs_size(name_locator, new_size, new_hot_size)
            self.client.wait_task_until_complete(result.get('task_id'))
        else:
            # dtree场景
            gfs_name = constants.SHARE_PREFIX + self.share_parent_id
            dtree_name = constants.SHARE_PREFIX + self.share.get('share_id')
            name_locator = '@'.join([cluster_name, gfs_name, dtree_name])
            self._check_space_for_dtree(name_locator, new_size)
            result = self.client.change_gfs_dtree_size(name_locator, new_size)
            self.client.wait_task_until_complete(result.get('task_id'))

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

    def _check_space_for_gfs(self, name_locator, new_hard_size, new_hot_size):
        gfs_detail = self.client.query_gfs_detail(name_locator)
        org_hard_size_in_gb = self._get_quota_in_gb(gfs_detail)

        # 冷、热、总都不能缩
        if new_hot_size:
            org_hot_size_in_gb = self._get_tier_limit(gfs_detail, 'tier_hot_limit')
            org_cold_size_in_gb = self._get_tier_limit(gfs_detail, 'tier_cold_limit')

            new_cold_size = new_hard_size - new_hot_size
            if new_cold_size <= org_cold_size_in_gb:
                err_msg = _("not allowed to shrinkage, new_cold_size: {0}, org_cold_size_in_gb: {1}")
                LOG.info(err_msg)
                raise exception.InvalidShare(reason=err_msg)

            if new_hot_size <= org_hot_size_in_gb:
                err_msg = _("not allowed to shrinkage, new_hot_size: {0}, org_hot_size_in_gb: {1}")
                LOG.info(err_msg)
                raise exception.InvalidShare(reason=err_msg)

        if new_hard_size <= org_hard_size_in_gb:
            err_msg = _("not allowed to shrinkage, new_hard_size: {0}, org_hard_size_in_gb: {1}")
            LOG.info(err_msg)
            raise exception.InvalidShare(reason=err_msg)

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
            'gfs_name_locator': self.storage_pool_name + '@' + self.namespace_name,
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
