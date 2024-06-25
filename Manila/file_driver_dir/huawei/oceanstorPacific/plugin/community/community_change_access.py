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
from manila.common import constants as common_constants
from manila.i18n import _

from ..change_access import ChangeAccess
from ...utils import constants

LOG = log.getLogger(__name__)


class CommunityChangeAccess(ChangeAccess):

    def __init__(self, client, share=None, driver_config=None,
                 context=None, storage_features=None):
        super(CommunityChangeAccess, self).__init__(
            client, share, driver_config, context, storage_features)
        self.namespace_name = None
        self.namespace_id = None
        self.share_path = None
        self.export_locations = None  # share路径信息
        self.nfs_share_id = None
        self.cifs_share_id = None
        self.access_proto = None
        self.nfs_rules = []
        self.cifs_rules = []
        self.dpc_rules = []

    @staticmethod
    def get_impl_type():
        return constants.PLUGIN_COMMUNITY_IMPL

    def update_access(self, access_rules, add_rules, delete_rules):
        """Update access rules list."""
        self._get_account_and_namespace_information()
        self._update_access_for_share(access_rules, add_rules, delete_rules)

    def allow_access(self, access):
        self._get_account_and_namespace_information()
        self._classify_rules([access], 'allow')

    def deny_access(self, access):
        self._get_account_and_namespace_information()
        self._classify_rules([access], 'deny')

    def _get_account_and_namespace_information(self):
        self._get_account_id()
        self._get_export_location_info()
        self._get_share_related_info()
        self._query_and_set_share_info()

    def _classify_rules(self, rules, action):

        for access in rules:
            access_type = access['access_type']
            if 'NFS' in self.access_proto and access_type == 'ip':
                self.nfs_rules.append(access)

            if 'CIFS' in self.access_proto and access_type == 'user':
                self.cifs_rules.append(access)

            if 'DPC' in self.access_proto and access_type == 'ip':
                self.dpc_rules.append(access)

        if self.nfs_rules:
            self._deal_access_for_nfs(action)
        if self.cifs_rules:
            self._deal_access_for_cifs(action)
        if self.dpc_rules:
            self._deal_access_for_dpc(action)

    def _deal_access_for_nfs(self, action):
        if action == 'allow':
            for access in self.nfs_rules:
                access_to = self.standard_ipaddr(access.get('access_to'))
                access_level = access.get('access_level')

                if access_level not in common_constants.ACCESS_LEVELS:
                    err_msg = _('Unsupported level of access was provided - {0}'.format(access_level))
                    raise exception.InvalidShareAccess(reason=err_msg)

                self.client.allow_access_for_nfs(self.nfs_share_id, access_to, access_level, self.account_id)

        elif action == 'deny':
            nfs_share_clients = {}
            result = self.client.query_nfs_share_clients_information(self.nfs_share_id, self.account_id)
            for data in result:
                access_name = self.standard_ipaddr(data.get('access_name'))
                nfs_share_clients[access_name] = data.get('id')

            for access in self.nfs_rules:
                access_to = self.standard_ipaddr(access.get('access_to'))

                if access_to in nfs_share_clients:
                    self.client.deny_access_for_nfs(nfs_share_clients[access_to], self.account_id)
                else:
                    LOG.info(_("The access_to {0} does not exist").format(access_to))

    def _deal_access_for_cifs(self, action):
        if action == 'allow':
            for access in self.cifs_rules:

                access_to = access.get('access_to')
                access_level = access.get('access_level')

                if access_level not in common_constants.ACCESS_LEVELS:
                    err_msg = _('Unsupported level of access was provided - {0}'.format(access_level))
                    raise exception.InvalidShareAccess(reason=err_msg)

                self.client.allow_access_for_cifs(self.cifs_share_id, access_to, access_level, self.account_id)
        elif action == 'deny':
            cifs_share_clients = {}
            result = self.client.query_cifs_share_user_information(self.cifs_share_id, self.account_id)
            for data in result:
                cifs_share_clients[data.get('name')] = data.get('id')

            for access in self.cifs_rules:
                access_to = access.get('access_to')
                if access_to in cifs_share_clients:
                    self.client.deny_access_for_cifs(cifs_share_clients[access_to], self.account_id)
                else:
                    LOG.info(_("The access_to {0} does not exist").format(access_to))

    def _get_dpc_access_ips_list(self):
        """
        Every 200 DPC IP addresses are grouped.
        :return:
        """
        dpc_access_ips_list = []
        for index in range(0, len(self.dpc_rules), 200):
            dpc_ips = []
            for access in self.dpc_rules[index:index + 200]:
                access_to = self.standard_ipaddr(access.get('access_to'))
                dpc_ips.append(access_to)
            dpc_access_ips_list.append(dpc_ips)

        return dpc_access_ips_list

    def _deal_access_for_dpc(self, action):
        """
        allow or deny dpc ips for dpc namespace
        :param action: 'allow' or 'deny'
        :return:
        """
        dpc_access_ips_list = self._get_dpc_access_ips_list()

        for dpc_ips in dpc_access_ips_list:
            if not dpc_ips:
                continue

            if action == "allow":
                LOG.info("Will be add dpc access.(nums: {0})".format(len(dpc_ips)))
                self.client.allow_access_for_dpc(self.namespace_name, ','.join(dpc_ips))
            else:
                LOG.info("Will be remove dpc access.(nums: {0})".format(len(dpc_ips)))
                self.client.deny_access_for_dpc(self.namespace_name, ','.join(dpc_ips))

    def _sync_access(self, access_rules):
        """Sync all access rules of the share between storage and platform"""
        access_value_key = 'access_value'
        client_id_key = 'access_value'
        if 'NFS' in self.access_proto:
            result = self.client.query_nfs_share_clients_information(self.nfs_share_id, self.account_id)
            deny_rules, allow_rules, change_rules = self._get_need_update_access(
                result, access_rules, 'access_name', access_value_key)
            for deny_rule in deny_rules:
                self.client.deny_access_for_nfs(deny_rule.get(client_id_key), self.account_id)
            for allow_rule in allow_rules:
                self.client.allow_access_for_nfs(
                    self.nfs_share_id, allow_rule.get('access_to'),
                    allow_rule.get('access_level'), self.account_id)
            for change_rule in change_rules:
                self.client.change_access_for_nfs(
                    change_rule.get(client_id_key),
                    change_rule.get(access_value_key), self.account_id)
        if 'CIFS' in self.access_proto:
            result = self.client.query_cifs_share_user_information(self.cifs_share_id, self.account_id)
            deny_rules, allow_rules, change_rules = self._get_need_update_access(
                result, access_rules, 'name', 'permission')
            for deny_rule in deny_rules:
                self.client.deny_access_for_cifs(deny_rule.get(client_id_key), self.account_id)
            for allow_rule in allow_rules:
                self.client.allow_access_for_cifs(
                    self.cifs_share_id, allow_rule.get('access_to'),
                    allow_rule.get('access_level'), self.account_id)
            for change_rule in change_rules:
                self.client.change_access_for_cifs(
                    change_rule.get(client_id_key),
                    change_rule.get(access_value_key), self.account_id)
        if 'DPC' in self.access_proto:
            self.client.deny_access_for_dpc(self.namespace_name, '*')
            self._classify_rules(access_rules, 'allow')

    def _get_need_update_access(self, storage_access_list, access_rules, access_param,
                                permission_param):
        """get all need deny access rules/allow access rules/change access rules"""
        need_remove_access_info = {}
        need_add_access_info = {}
        need_change_access_info = {}
        access_to_key = 'access_to'
        access_level_key = 'access_level'
        client_id_key = 'client_id'
        for data in storage_access_list:
            access_name = self.standard_ipaddr(data.get(access_param))
            need_remove_access_info[access_name] = {
                access_to_key: access_name,
                access_level_key: data.get(permission_param),
                client_id_key: data.get('id')
            }
        for rule in access_rules:
            access_to = self.standard_ipaddr(rule.get(access_to_key))
            access_level = 0 if rule.get(access_level_key) == 'ro' else 1
            access_info = need_remove_access_info.get(access_to)
            if not access_info:
                need_add_access_info[access_to] = {
                    access_to_key: access_to,
                    access_level_key: rule.get(access_level_key)
                }
            elif access_info.get(access_level_key) != access_level:
                need_change_access_info[access_to] = {
                    client_id_key: access_info.get(client_id_key),
                    'access_value': rule.get(access_level_key),
                }
                need_remove_access_info.pop(access_to)
            else:
                need_remove_access_info.pop(access_to)

        return need_remove_access_info, need_add_access_info, need_change_access_info

    def _get_export_location_info(self):
        """校验share是否包含path信息，有则初始化"""
        export_locations_key = "export_locations"
        if (not self.share.get(export_locations_key) or not self.share.get(
                export_locations_key)[0].get('path')):
            err_msg = _("share fail for invalid export location.")
            raise exception.InvalidShare(reason=err_msg)
        self.export_locations = self.share.get(export_locations_key)[0].get('path')

    def _get_share_related_info(self):
        """获取命名空间的名称和share的路径信息"""

        self.namespace_name = self.export_locations.split('\\')[-1].split('/')[-1]
        self.share_path = '/' + self.namespace_name + '/'
        result = self.client.query_namespace_by_name(self.namespace_name)
        self.namespace_id = result.get('id')

    def _query_and_set_share_info(self, dtree_id=0, dtree_name=None):
        """根据share_path信息查询对应的share信息"""

        self.access_proto = self._get_share_access_proto()
        if 'NFS' in self.access_proto:
            result = self.client.query_nfs_share_information(
                self.account_id, self.namespace_id, dtree_id)
            for nfs_share in result:
                if self.share_path == nfs_share.get('share_path'):
                    self.nfs_share_id = nfs_share.get('id')
                    break
            else:
                err_msg = _("Cannot get NFS share id(namespace_name:{0}).".format(self.namespace_name))
                raise exception.InvalidShare(reason=err_msg)

        if 'CIFS' in self.access_proto:
            result = self.client.query_cifs_share_information(
                self.account_id, dtree_name if dtree_name else self.namespace_name)
            for cifs_share in result:
                if self.share_path == cifs_share.get('share_path'):
                    self.cifs_share_id = cifs_share.get('id')
                    break
            else:
                err_msg = _("Cannot get CIFS share id(namespace_name:{0}).".format(self.namespace_name))
                raise exception.InvalidShare(reason=err_msg)

    def _update_access_for_share(self, access_rules, add_rules, delete_rules):
        """根据传入的参数为共享添加或者移除权限"""

        if add_rules:
            self._classify_rules(add_rules, 'allow')
        if delete_rules:
            self._classify_rules(delete_rules, 'deny')
        if not (add_rules or delete_rules):
            self._sync_access(access_rules)