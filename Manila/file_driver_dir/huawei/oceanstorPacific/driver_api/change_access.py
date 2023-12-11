# coding=utf-8
# Copyright (c) 2021 Huawei Technologies Co., Ltd.
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

import netaddr
from oslo_log import log

from manila import exception
from manila.common import constants as common_constants
from manila.i18n import _


LOG = log.getLogger(__name__)


class ChangeAccess(object):
    def __init__(self, helper, share):
        self.helper = helper
        self.share = share

        self.account_id = None
        self.namespace_name = None
        self.share_proto = self.share.get('share_proto', '').split('&')
        self.share_path = None
        self.export_locations = None  # share路径信息
        self.nfs_share_id = None
        self.cifs_share_id = None
        self.nfs_rules = []
        self.cifs_rules = []

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

    def _find_account_id(self):
        account_name = self.share.get('project_id')
        result = self.helper.query_account_by_name(account_name)
        self.account_id = result.get('id')

    def _get_account_and_namespace_information(self):
        self._find_account_id()
        self._get_export_location_info()
        self._get_share_related_info()
        self._query_and_set_share_info()

    def _classify_rules(self, rules, action):

        for access in rules:
            access_type = access['access_type']
            if 'NFS' in self.share_proto and access_type == 'ip':
                self.nfs_rules.append(access)

            if 'CIFS' in self.share_proto and access_type == 'user':
                self.cifs_rules.append(access)

        if self.nfs_rules:
            self._deal_access_for_nfs(action)
        if self.cifs_rules:
            self._deal_access_for_cifs(action)

    def _deal_access_for_nfs(self, action):
        if action == 'allow':
            for access in self.nfs_rules:
                access_to = access['access_to']
                access_level = access['access_level']

                if not self._check_ip_valid(access_to):
                    message = _('The access_to is invalid.')
                    raise exception.InvalidInput(reason=message)

                if access_level not in common_constants.ACCESS_LEVELS:
                    err_msg = _('Unsupported level of access was provided - {0}'.format(access_level))
                    raise exception.InvalidShareAccess(reason=err_msg)

                self.helper.allow_access_for_nfs(self.nfs_share_id, access_to, access_level, self.account_id)

        elif action == 'deny':
            nfs_share_clients = {}
            result = self.helper.query_nfs_share_clients_information(self.nfs_share_id, self.account_id)
            for data in result:
                nfs_share_clients[data['access_name']] = data['id']

            for access in self.nfs_rules:
                access_to = access['access_to']
                if not self._check_ip_valid(access_to):
                    LOG.warning(_('The access_to is invalid, ignored.'))
                    continue

                if access_to in nfs_share_clients:
                    self.helper.deny_access_for_nfs(nfs_share_clients[access_to], self.account_id)
                else:
                    LOG.info(_("The access_to {0} does not exist").format(access_to))

    def _deal_access_for_cifs(self, action):
        if action == 'allow':
            for access in self.cifs_rules:

                access_to = access['access_to']
                access_level = access['access_level']

                if access_level not in common_constants.ACCESS_LEVELS:
                    err_msg = _('Unsupported level of access was provided - {0}'.format(access_level))
                    raise exception.InvalidShareAccess(reason=err_msg)

                self.helper.allow_access_for_cifs(self.cifs_share_id, access_to, access_level, self.account_id)
        elif action == 'deny':
            cifs_share_clients = {}
            result = self.helper.query_cifs_share_user_information(self.cifs_share_id, self.account_id)
            for data in result:
                cifs_share_clients[data['name']] = data['id']

            for access in self.cifs_rules:
                access_to = access['access_to']
                if access_to in cifs_share_clients:
                    self.helper.deny_access_for_cifs(cifs_share_clients[access_to], self.account_id)
                else:
                    LOG.info(_("The access_to {0} does not exist").format(access_to))

    def _clear_access(self):
        """Remove all access rules of the share"""
        if 'NFS' in self.share_proto:
            result = self.helper.query_nfs_share_clients_information(self.nfs_share_id, self.account_id)
            for data in result:
                self.helper.deny_access_for_nfs(data['id'], self.account_id)
        if self.share_proto == 'CIFS':
            result = self.helper.query_cifs_share_user_information(self.cifs_share_id, self.account_id)
            for data in result:
                self.helper.deny_access_for_cifs(data['id'], self.account_id)

    @staticmethod
    def _check_ip_valid(ip):
        """
        rules for IP address authorization scenarios:
        1. only supports one IPv4 address or address segment.
        2. when IP is address segment, format is "ip/mask", the range of
           subnet mask is [1, 31], but 0.0.0.0/0 is special,
           0.0.0.0/0 is allowed.
        3. the follow ip or ip address segment is not allowed:
           0.*.*.*, 0.*.*.*/*, 127.*.*.*, 127.*.*.*/*,
           224~255.*.*.*, 224~255.*.*.*/*

           :param ip: ip or ip segment
           :return: True is ip valid, else False
        """
        if ip == "0.0.0.0/0":
            return True

        ip_set = ip.split("/")
        ip_set_len = len(ip_set)
        if ip_set_len == 2:
            ip = ip_set[0]
            subnet_mask = ip_set[1]
        elif ip_set_len == 1:
            ip = ip_set[0]
            subnet_mask = ""
        else:
            return False

        if ip_set_len == 2:
            if subnet_mask == "" or not subnet_mask.isdigit():
                return False

            if int(subnet_mask) < 1 or int(subnet_mask) > 31:
                return False

        if len(ip.split(".")) != 4:
            return False

        # note: netaddr.valid_ipv4() method not valid num of ip, which means
        # 1.1.1 is valid for valid_ipv4() method
        if not netaddr.valid_ipv4(ip):
            return False

        ip_1st_colum = int(ip.split(".")[0])
        ip_not_allowed_address_segment = (224 <= ip_1st_colum <= 255)
        if ip_1st_colum == 0 or ip_1st_colum == 127 or ip_not_allowed_address_segment:
            return False

        return True

    def _get_export_location_info(self):
        """校验share是否包含path信息，有则初始化"""

        if not self.share.get('export_locations') or not self.share.get('export_locations')[0].get('path'):
            err_msg = _("share fail for invalid export location.")
            raise exception.InvalidShare(reason=err_msg)
        self.export_locations = self.share.get('export_locations')[0].get('path')

    def _get_share_related_info(self):
        """获取命名空间的名称和share的路径信息"""

        self.namespace_name = self.export_locations.split('\\')[-1].split('/')[-1]
        self.share_path = '/' + self.namespace_name + '/'

    def _query_and_set_share_info(self):
        """根据share_path信息查询对应的share信息"""

        if 'NFS' in self.share_proto:
            result = self.helper.query_nfs_share_information(self.account_id)
            for nfs_share in result:
                if self.share_path == nfs_share.get('share_path'):
                    self.nfs_share_id = nfs_share.get('id')
                    break
            else:
                err_msg = _("Cannot get NFS share id(namespace_name:{0}).".format(self.namespace_name))
                raise exception.InvalidShare(reason=err_msg)

        if 'CIFS' in self.share_proto:
            result = self.helper.query_cifs_share_information(self.account_id)
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
            self._clear_access()
            self._classify_rules(access_rules, 'allow')
