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

from oslo_log import log
from manila.i18n import _
from manila import exception
from manila.common import constants as common_constants
from ..community.community_change_access import CommunityChangeAccess
from ...utils import constants

LOG = log.getLogger(__name__)


class DmeChangeAccess(CommunityChangeAccess):
    def __init__(self, client, share=None, driver_config=None, context=None, storage_features=None):
        super(DmeChangeAccess, self).__init__(client, share, driver_config, context, storage_features)
        self.share_parent_id = self._get_share_parent_id()
        self.managed_storage_type = []

    @staticmethod
    def get_impl_type():
        return constants.PLUGIN_DME_FILESYSTEM_IMPL, None

    @staticmethod
    def _is_need_to_update_access(share_clients, access_to, access_level):
        if share_clients.get('type') == 'DPC':
            return False
        access_value = 'read' if access_level == 'ro' else 'read_and_write'
        if share_clients.get(access_to).get('permission') != access_value:
            return True
        return False

    def update_access(self, access_rules, add_rules, delete_rules):
        self._set_managed_storage()
        if add_rules:
            self._get_share_access_proto(add_rules, True)
        if delete_rules:
            self._get_share_access_proto(delete_rules, False)
        if not add_rules and not delete_rules:
            self._get_share_access_proto(access_rules, True)
        self._get_share_id()
        self._update_access_for_share(add_rules, delete_rules)

    def allow_access(self, access):
        self._set_managed_storage()
        self._get_share_access_proto([access], True)
        self._get_share_id()
        self._classify_rules(self.allow_access_proto, 'allow')

    def deny_access(self, access):
        self._set_managed_storage()
        self._get_share_access_proto([access], False)
        self._get_share_id()
        self._classify_rules(self.deny_access_proto, 'deny')

    def _classify_rules(self, access_rules, action):
        access_type_key = 'access_type'
        self.nfs_rules = []
        self.dpc_rules = []
        nfs_access_rules = access_rules.get('NFS', [])
        for nfs_access_rule in nfs_access_rules:
            if nfs_access_rule.get(access_type_key) == 'ip':
                self.nfs_rules.append(nfs_access_rule)
        dpc_access_rules = access_rules.get('DPC', [])
        for dpc_access_rule in dpc_access_rules:
            if dpc_access_rule.get(access_type_key) == 'ip':
                self.dpc_rules.append(dpc_access_rule)

        if self.nfs_rules:
            self._deal_access_for_nfs(action)
        if self.dpc_rules:
            self._deal_access_for_dpc(action)

    def _deal_access_for_nfs(self, action):
        """
        处理NFS共享的访问控制
        Args:action (str): 操作类型，'allow' 或 'deny'
        """
        # 获取现有的NFS共享客户端信息
        nfs_share_clients = self._get_nfs_share_clients()

        # 根据操作类型处理访问控制
        if action == 'allow':
            self._handle_allow_access(nfs_share_clients, self.nfs_rules)
        elif action == 'deny':
            self._handle_deny_access(nfs_share_clients, self.nfs_rules)
        else:
            LOG.warning("Unsupported action type: %s", action)

    def _deal_access_for_dpc(self, action):
        """
        处理DPC共享的访问控制
        Args:action (str): 操作类型，'allow' 或 'deny'
        """
        # 获取现有的NFS共享客户端信息
        dpc_share_clients = self._get_dpc_share_clients()

        # 根据操作类型处理访问控制
        if action == 'allow':
            self._handle_allow_access(dpc_share_clients, self.dpc_rules)
        elif action == 'deny':
            self._handle_deny_access(dpc_share_clients, self.dpc_rules)
        else:
            LOG.warning("Unsupported action type: %s", action)

    def _get_nfs_share_clients(self):
        """获取NFS共享客户端信息"""
        nfs_share_clients = {'type': 'NFS'}
        if not self.nfs_share_id:
            return nfs_share_clients
        try:
            access_client = self.client.get_nfs_share_clients({'nfs_share_id': self.nfs_share_id})
            for data in access_client:
                access_name = self.standard_ipaddr(data.get('name', ''))
                client_id = data.get('client_id_in_storage')
                permission = data.get('permission')
                if access_name and client_id and permission:
                    nfs_share_clients[access_name] = {
                        'client_id': client_id, 'permission': permission}
            return nfs_share_clients
        except Exception as e:
            LOG.error("Failed to get NFS share clients: %s", str(e))
            raise

    def _get_dpc_share_clients(self):
        """获取DPC共享客户端信息"""
        dpc_share_clients = {'type': 'DPC'}
        if not self.dpc_share_id:
            return dpc_share_clients
        try:
            access_client = self.client.get_dpc_share_clients(self.dpc_share_id)
            for data in access_client:
                access_name = self.standard_ipaddr(data.get('ip_address', ''))
                client_id = data.get('auth_raw_id')
                if access_name and client_id:
                    dpc_share_clients[access_name] = {'client_id': client_id}
            return dpc_share_clients
        except Exception as e:
            LOG.error("Failed to get DPC share clients: %s", str(e))
            raise

    def _handle_allow_access(self, share_clients, share_rules):
        """处理允许访问的逻辑"""
        for access in share_rules:
            access_to = self.standard_ipaddr(access.get('access_to', ''))
            access_level = access.get('access_level')
            if access_level not in common_constants.ACCESS_LEVELS:
                err_msg = _('Unsupported level of access was provided - {0}').format(access_level)
                raise exception.InvalidShareAccess(reason=err_msg)

            # 处理访问控制
            if access_to not in share_clients:
                self._add_new_access(access_to, access_level, share_clients.get('type'))
            elif self._is_need_to_update_access(share_clients, access_to, access_level):
                self._update_existing_access(
                    access_to, access_level, share_clients.get(access_to).get('client_id'))
            else:
                LOG.info(_("The access_to %s access level %s has already exist"), access_to, access_level)

    def _add_new_access(self, access_to, access_level, proto_type):
        """添加新的访问控制"""
        try:
            if proto_type == 'NFS':
                task_id = self.client.allow_access_for_nfs(self.nfs_share_id, access_to, access_level)
            else:
                task_id = self.client.allow_access_for_dpc(self.dpc_share_id, access_to)
            self.client.wait_task_until_complete(
                task_id, query_interval_seconds=constants.DME_QUERY_INTERVAL_SECONDS)
            LOG.info("Successfully added access for %s with level %s", access_to, access_level)
        except Exception as e:
            LOG.error("Failed to add access for %s: %s", access_to, str(e))
            raise

    def _update_existing_access(self, access_to, access_level, client_id):
        """更新已存在的访问控制"""
        try:
            task_id = self.client.update_access_for_nfs(self.nfs_share_id, client_id, access_level)
            self.client.wait_task_until_complete(
                task_id, query_interval_seconds=constants.DME_QUERY_INTERVAL_SECONDS)
            LOG.info("Successfully updated access for %s with level %s", access_to, access_level)
        except Exception as e:
            LOG.error("Failed to update access for %s: %s", access_to, str(e))
            raise

    def _handle_deny_access(self, share_clients, share_rules):
        """处理拒绝访问的逻辑"""
        for access in share_rules:
            access_to = self.standard_ipaddr(access.get('access_to', ''))

            if access_to in share_clients:
                self._remove_access(
                    access_to, share_clients[access_to].get('client_id'),
                    share_clients.get('type'))
            else:
                LOG.info(_("The access_to %s does not exist"), access_to)

    def _remove_access(self, access_to, client_id, proto_type):
        """移除访问控制"""
        try:
            if proto_type == 'NFS':
                task_id = self.client.deny_access_for_nfs(self.nfs_share_id, access_to, client_id)
            else:
                task_id = self.client.deny_access_for_dpc(self.dpc_share_id, client_id)
            self.client.wait_task_until_complete(
                task_id, query_interval_seconds=constants.DME_QUERY_INTERVAL_SECONDS)
            LOG.info("Successfully denied access for %s", access_to)
        except Exception as e:
            LOG.error("Failed to deny access for %s: %s", access_to, str(e))
            raise

    def _build_query_param(self):
        return {
            'storage_id': self.driver_config.A800.storage_id,
            'zone_id': (
                self.driver_config.A800.zone_id
                if self.driver_config.A800.zone_id
                else self.driver_config.A800.storage_id
            ),
            'vstore_raw_id': self.driver_config.A800.vstore_raw_id,
            'name': 'share-' + self.share.get('share_id')
        }

    def _get_share_id(self):
        """
        添加权限场景，如果没查询到share，Driver报错
        :return:
        """
        share_name = 'share-' + self.share.get('share_id')
        self.share_path = share_name
        if self.share_parent_id:
            param = self._get_secondary_share_query_param()
            self.share_path = 'share-' + self.share_parent_id + '/' + share_name
        elif 'A800' in self.managed_storage_type:
            file_system = self.client.query_specified_file_system(self._build_query_param())
            param = {'fs_id': file_system.get('id')}
        elif 'Pacific' in self.managed_storage_type:
            namespace = self.client.query_specified_namespaces(self._build_query_namespace_param())
            param = {'namespace_id': namespace.get('id')}
        else:
            error_msg = "Can not find storage config."
            LOG.error(error_msg)
            raise exception.InvalidShare(reason=error_msg)

        if 'NFS' in self.allow_access_proto or 'NFS' in self.deny_access_proto:
            nfs_shares = self.client.get_nfs_share(param)
            self._check_share(nfs_shares, 'NFS')
            self.nfs_share_id = self._set_share_id(nfs_shares)
        if 'DPC' in self.allow_access_proto or 'DPC' in self.deny_access_proto:
            dpc_shares = self.client.get_dpc_share(param)
            self._check_share(dpc_shares, 'DPC')
            self.dpc_share_id = self._set_share_id(dpc_shares)

    def _check_share(self, share_list, share_proto):
        if share_proto in self.allow_access_proto and len(share_list) == 0:
            err_msg = "Expected at least 1 %s share, but got %s." % (share_proto, len(share_list))
            raise exception.InvalidShare(reason=err_msg)

    def _set_share_id(self, share_list):
        if not share_list:
            return ''
        for share in share_list:
            share_path = share.get('share_path', '').strip('/')
            if share_path == self.share_path:
                return share.get('id')
        return ''

    def _build_query_namespace_param(self):
        return {
            'storage_id': self.driver_config.Pacific.storage_id,
            'vstore_raw_id': self.driver_config.Pacific.vstore_raw_id,
            'name': 'share-' + self.share.get('share_id')
        }

    def _get_secondary_share_query_param(self):
        filesystem_dtree = {}
        namespace_dtree = {}
        if self.driver_config.A800:
            filesystem_dtree = self.client.query_specified_dtree(self._build_query_param())
        if filesystem_dtree:
            return {'owning_dtree_id': filesystem_dtree.get('id')}

        if self.driver_config.Pacific:
            namespace_dtree = self.client.query_specified_dtree({
                'storage_id': self.driver_config.Pacific.storage_id,
                'vstore_raw_id': self.driver_config.Pacific.vstore_raw_id,
                'name': 'share-' + self.share.get('share_id')
            })
        if namespace_dtree:
            return {'owning_dtree_id': namespace_dtree.get('id')}

        error_msg = "Share %s not exist on device" % self.share.get('share_id')
        LOG.error(error_msg)
        raise exception.InvalidShare(reason=error_msg)