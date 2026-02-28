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

from datetime import date

from oslo_log import log
from manila import exception
from manila.i18n import _
from ..community.community_operate_share import CommunityOperateShare
from ...utils import constants, driver_utils

LOG = log.getLogger(__name__)


class DmeOperateShare(CommunityOperateShare):
    def __init__(self, client, share=None, driver_config=None, context=None, storage_features=None):
        super(DmeOperateShare, self).__init__(client, share, driver_config, context, storage_features)
        self.share_parent_id = self._get_share_parent_id()
        self.filesystem_size = None

    @staticmethod
    def get_impl_type():
        return constants.PLUGIN_DME_FILESYSTEM_IMPL, None

    def create_share(self):
        if not self.share_parent_id:
            return self._create_file_system()
        else:
            return self._create_dtree()

    def delete_share(self):
        param = self._build_query_param()
        if not self.share_parent_id:
            return self._delete_file_system(param)
        else:
            return self._delete_dtree(param)

    def change_share(self, new_size, action):
        param = self._build_query_param()
        if not self.share_parent_id:
            return self._change_file_system_size(param, new_size)
        else:
            return self._change_dtree_size(param, new_size)

    def update_qos(self, qos_specs):
        self._set_share_to_share_instance()
        param = self._build_query_param()
        if not self.share_parent_id:
            return self._update_qos(param, qos_specs)
        else:
            param.update({'name': 'share-' + self.share_parent_id})
            return self._update_qos(param, qos_specs)

    def get_share_usage(self, share_usages):
        if not self.share.get('share_id'):
            err_msg = _("There is no share_id attribution in share object:%s") % self.share
            LOG.error(err_msg)
            raise exception.InvalidShare(reason=err_msg)

        share_usage = share_usages.get(self.share.get('share_id'), {})
        if not share_usage:
            LOG.info("Can not find share in share_usages.")
            return {}

        LOG.info("Get share usage:%s of share:%s from share_usages successfully",
                 share_usage, self.share.get('share_id'))
        return share_usage

    def parse_cmcc_qos_options(self):
        if not self.share_parent_id:
            size = self.share.get('size')
        else:
            size = self.filesystem_size if self.filesystem_size is not None else self.share.get('size')
        qos = self._set_qos_param_by_size_and_type(size)
        LOG.info('share size is %s, the default qos is %s', size, qos)
        return qos

    def ensure_share(self):
        return self._get_ensure_share_location()

    def _get_current_storage_pool_id(self):
        return self.driver_config.pool_raw_id

    def _create_file_system(self):
        self._check_create_fs_param()
        fs_name = 'share-' + self.share.get('share_id')
        create_fs_request = self._build_create_fs_request(fs_name)
        task_id = self.client.create_file_system(create_fs_request)
        self.client.wait_task_until_complete(task_id, query_interval_seconds=1)
        return self._get_share_path(fs_name + '/')

    def _build_create_fs_request(self, fs_name):
        create_fs_param = {
            'storage_id': self.driver_config.storage_id,
            'pool_raw_id': self.driver_config.pool_raw_id,
            'vstore_id': self.driver_config.vstore_id,
            'filesystem_specs': [{'name': fs_name, 'capacity': self.share.get('size'), 'count': 1}]
        }
        if self.driver_config.zone_id:
            create_fs_param['zone_id'] = self.driver_config.zone_id
        else:
            create_fs_param['zone_id'] = self.driver_config.storage_id

        if 'NFS' in self.share_proto:
            create_nfs_share_param = {
                'share_path': '/' + fs_name + '/',
                'character_encoding': self.driver_config.nfs_charset
            }
            create_fs_param['create_nfs_share_param'] = create_nfs_share_param
        if 'DPC' in self.share_proto:
            create_dpc_share_param = {
                'charset': self.driver_config.dpc_charset,
                'dpc_share_auth': [
                    {
                        'dpc_user_id': self.driver_config.dpc_user_id,
                        'permission': self.driver_config.dpc_user_permission
                    }
                ]
            }
            create_fs_param['create_dpc_share_param'] = create_dpc_share_param
        max_qos_info = self.parse_cmcc_qos_options()
        qos_param = self._build_qos_param(0, 0, max_qos_info)
        create_fs_param.update(qos_param)
        return create_fs_param

    def _create_dtree(self):
        self._check_create_dtree_param()
        file_system_detail = self._get_file_system_detail()
        dtree_name = 'share-' + self.share.get('share_id')
        fs_id = file_system_detail.get('id')
        fs_name = file_system_detail.get('name')
        # 创建Dtree
        create_dtree_param = self._get_create_dtree_param(dtree_name, fs_id, fs_name)
        dtree_task_id = self.client.create_dtree(create_dtree_param)
        self.client.wait_task_until_complete(dtree_task_id, query_interval_seconds=0.5)
        # 创建配额
        dtree_detail = self._get_dtree_detail(dtree_name, fs_id)
        quota_task_id = self._create_dtree_quota(dtree_detail['id'])
        self.client.wait_task_until_complete(quota_task_id, query_interval_seconds=0.5)
        return self._get_share_path(fs_name + '/' + dtree_name)

    def _get_dtree_detail(self, dtree_name, fs_id):
        query_dtree_param = {
            'name': dtree_name,
            'storage_id': self.driver_config.storage_id,
            'zone_id': self.driver_config.zone_id,
            'fs_id': fs_id
        }
        return self.client.get_dtree_by_name_and_vstore(query_dtree_param, dtree_name, self.driver_config.vstore_id)

    def _get_file_system_detail(self):
        file_name = 'share-' + self.share_parent_id
        file_system_param = {
            'storage_id': self.driver_config.storage_id,
            'zone_id': self.driver_config.zone_id,
            'name': file_name,
            'vstore_raw_id': self.driver_config.vstore_raw_id
        }
        return self.client.get_file_system_by_name(file_system_param, file_name)

    def _get_create_dtree_param(self, dtree_name, fs_id, fs_name):
        create_dtree_param = {
            'storage_id': self.driver_config.storage_id,
            'zone_id': self.driver_config.zone_id,
            'fs_id': fs_id,
            'security_mode': self.driver_config.security_mode,
            'quota_switch': True,
            'create_dtrees_param': [{'dtree_name': dtree_name, 'count': 1}]
        }
        share_path = '/' + fs_name + '/' + dtree_name
        if 'NFS' in self.share_proto:
            nfs_share_param = {
                "share_path": share_path,
                "character_encoding": self.driver_config.nfs_charset
            }
            create_dtree_param['create_nfs_share_param'] = nfs_share_param
        if 'DPC' in self.share_proto:
            dataturbo_share = {
                "charset": self.driver_config.dpc_charset,
                "dpc_share_auth": [{
                    "permission": self.driver_config.dpc_user_permission,
                    "dpc_user_id": self.driver_config.dpc_user_id
                }]
            }
            create_dtree_param['dataturbo_share'] = dataturbo_share
        return create_dtree_param

    def _create_dtree_quota(self, dtree_id):
        create_quota_param = {
            'parent_id': dtree_id,
            'parent_type': 'dtree',
            'space_hard_quota': self.share.get('size') * constants.CAPACITY_UNIT_BYTE_TO_GB,
            'quota_type': 'directory_quota'
        }
        return self.client.create_quota(create_quota_param)

    def _check_create_dtree_param(self):
        self._check_storage_id()
        self._check_vstore_id()
        self._check_zone_id_required()
        self._check_dpc_param()

    def _check_create_fs_param(self):
        self._check_storage_id()
        self._check_vstore_id()
        self._check_zone_id()
        self._check_dpc_param()

    def _check_storage_id(self):
        """storageId是根据storageSn查询出来的,可能为空"""
        if not self.driver_config.storage_id:
            raise exception.InvalidInput('Failed to create the share because the storage ID is empty.')

    def _check_zone_id(self):
        """zoneId是根据zoneRawId查询出来的,可能为空"""
        if self.driver_config.zone_raw_id and not self.driver_config.zone_id:
            raise exception.InvalidInput('Failed to create the share because the zone ID is empty.')

    def _check_zone_id_required(self):
        """zoneId是根据zoneRawId查询出来的,可能为空"""
        if not self.driver_config.zone_raw_id or not self.driver_config.zone_id:
            raise exception.InvalidInput('Failed to create the share because the zone ID is empty.')

    def _check_vstore_id(self):
        """vstoreId是根据vstoreRawId查询出来的,可能为空"""
        if not self.driver_config.vstore_id:
            raise exception.InvalidInput('Failed to create the share because the vstore ID is empty.')

    def _check_dpc_param(self):
        """DPC USER ID是根据DPC USER名称查询出来的,可能为空"""
        if 'DPC' in self.share_proto and not self.driver_config.dpc_user_id:
            raise exception.InvalidInput('Failed to create the share because the dpc user ID is empty.')

    def _get_share_path(self, share_path):
        """返回共享路径"""
        location = []
        if 'DPC' in self.share_proto:
            location.append('dtfs:' + '/' + share_path)
        if 'NFS' in self.share_proto:
            location.append('NFS:' + "/" + share_path)

        LOG.info("Create share successfully, the location of this share is %s", location)
        return location

    def _update_qos(self, param, qos_specs):
        file_system = self.client.query_specified_file_system(param)
        self._get_update_qos_config(qos_specs)
        self.filesystem_size = file_system.get('total_capacity_in_byte', 0) / constants.CAPACITY_UNIT_BYTE_TO_GB
        max_qos_info = self.parse_cmcc_qos_options()

        iops = int(self.qos_config.get('max_iops', 0))
        bandwidth = int(self.qos_config.get('max_mbps', 0))

        qos_param = self._build_qos_param(iops, bandwidth, max_qos_info)
        task_id = self.client.update_file_system(file_system.get('id'), qos_param)
        self.client.wait_task_until_complete(task_id, query_interval_seconds=constants.DME_QUERY_INTERVAL_SECONDS)
        return True

    def _build_query_param(self):
        return {
            'storage_id': self.driver_config.storage_id,
            'zone_id': self.driver_config.zone_id if self.driver_config.zone_id else self.driver_config.storage_id,
            'vstore_raw_id': self.driver_config.vstore_raw_id,
            'name': 'share-' + self.share.get('share_id')
        }

    def _delete_file_system(self, param):
        file_system = self.client.query_specified_file_system(param)
        file_system_id = file_system.get('id')

        share_param = {'fs_id': file_system_id}
        self._delete_nfs_and_dpc_shares(share_param)

        file_system_task_id = self.client.delete_file_system([file_system_id])
        self.client.wait_task_until_complete(file_system_task_id,
                                             query_interval_seconds=constants.DME_QUERY_INTERVAL_SECONDS)

        return True

    def _delete_dtree(self, param):
        dtree = self.client.query_specified_dtree(param)
        dtree_id = dtree.get('id')

        share_param = {'owning_dtree_id': dtree_id, 'dtree_id': dtree_id}
        self._delete_nfs_and_dpc_shares(share_param)

        dtree_task_id = self.client.delete_dtree([dtree_id])
        self.client.wait_task_until_complete(dtree_task_id,
                                             query_interval_seconds=constants.DME_QUERY_INTERVAL_SECONDS)

        return True

    def _delete_nfs_and_dpc_shares(self, param):
        def del_nfs_shares():
            nfs_shares = self.client.get_nfs_share(param)
            nfs_share_ids = [obj.get('id') for obj in nfs_shares]
            if nfs_share_ids and len(nfs_share_ids) > 0:
                nfs_task_id = self.client.delete_nfs_share(nfs_share_ids)
                self.client.wait_task_until_complete(nfs_task_id,
                                                     query_interval_seconds=constants.DME_QUERY_INTERVAL_SECONDS)

        def del_dpc_shares():
            dpc_shares = self.client.get_dpc_share(param)
            dpc_share_ids = [obj.get('id') for obj in dpc_shares]
            if dpc_share_ids and len(dpc_share_ids) > 0:
                dpc_task_id = self.client.delete_dpc_share(dpc_share_ids)
                self.client.wait_task_until_complete(dpc_task_id,
                                                     query_interval_seconds=constants.DME_QUERY_INTERVAL_SECONDS)

        with ThreadPoolExecutor(max_workers=2) as executor:
            future_nfs = executor.submit(del_nfs_shares)
            future_dpc = executor.submit(del_dpc_shares)

            future_nfs.result()
            future_dpc.result()

    def _change_file_system_size(self, param, new_size):
        file_system = self.client.query_specified_file_system(param)
        file_system_id = file_system.get('id')

        max_qos_info = self._set_qos_param_by_size_and_type(new_size)
        update_param = self._build_qos_param(0, 0, max_qos_info)
        update_param.update({'capacity': new_size})
        LOG.info('share size is %s, the default qos is %s', new_size, max_qos_info)
        task_id = self.client.update_file_system(file_system_id, update_param)
        self.client.wait_task_until_complete(task_id, query_interval_seconds=0.5)

        return True

    def _change_dtree_size(self, param, new_size):
        dtree = self.client.query_specified_dtree(param)
        quota_param = {
            'quota_type': 'directory_quota',
            'parent_type': 'qtree',
            'parent_raw_id': dtree.get('id_in_storage')
        }
        quota = self.client.query_specified_quota(quota_param)
        update_quota_param = {'space_hard_quota': new_size * constants.CAPACITY_UNIT_BYTE_TO_GB}
        task_id = self.client.update_quota(quota.get('id'), update_quota_param)
        self.client.wait_task_until_complete(task_id, query_interval_seconds=0.5)

        return True

    def _build_qos_param(self, iops, bandwidth, max_qos_info):
        max_iops = iops
        if iops == 1:
            max_iops = self.driver_config.max_iops
        elif iops == 0:
            max_iops = int(max_qos_info.get('max_iops', 0))

        max_bandwidth = bandwidth
        if bandwidth == 1:
            max_bandwidth = self.driver_config.max_bandwidth
        elif bandwidth == 0:
            max_bandwidth = int(max_qos_info.get('max_mbps', 0))

        qos_param = {
            'tuning': {
                "qos_policy": {
                    "enabled": 'true',
                    "io_policy_type": "read_or_write_upper_limit",
                    "max_read_bandwidth": max_bandwidth,
                    "max_write_bandwidth": max_bandwidth,
                    "max_read_iops": max_iops,
                    "max_write_iops": max_iops,
                    "alarm_switch": "off",
                    "schedule_policy": "daily",
                    "duration": 86400,
                    "start_time": "00:00",
                    "schedule_start_date": date.today().strftime("%Y-%m-%d"),
                    "iotype": "3"
                }
            }
        }

        if max_iops == 0:
            qos_policy = qos_param.get('tuning').get('qos_policy')
            del qos_policy['max_read_iops']
            del qos_policy['max_write_iops']

        return qos_param
