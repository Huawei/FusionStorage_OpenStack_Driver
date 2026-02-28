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

    @staticmethod
    def get_impl_type():
        return constants.PLUGIN_DME_FILESYSTEM_IMPL, None

    def update_access(self, access_rules, add_rules, delete_rules):
        if add_rules:
            self._get_share_access_proto(add_rules, True)
        if delete_rules:
            self._get_share_access_proto(delete_rules, False)
        if not add_rules and not delete_rules:
            self._get_share_access_proto(access_rules, True)
        self._get_share_id()
        self._update_access_for_share(add_rules, delete_rules)

    def allow_access(self, access):
        self._get_share_access_proto([access], True)
        self._get_share_id()
        self._classify_rules([access], 'allow')

    def deny_access(self, access):
        self._get_share_access_proto([access], False)
        self._get_share_id()
        self._classify_rules([access], 'deny')

    def _classify_rules(self, access_rules, action):
        access_type_key = 'access_type'
        self.nfs_rules = []
        nfs_access_rules = access_rules.get('NFS', [])
        for nfs_access_rule in nfs_access_rules:
            if nfs_access_rule.get(access_type_key) == 'ip' or nfs_access_rule.get(access_type_key) == 'user':
                self.nfs_rules.append(nfs_access_rule)

        if self.nfs_rules:
            self._deal_access_for_nfs(action)

    def _deal_access_for_nfs(self, action):
        if action == 'allow':
            for access in self.nfs_rules:
                access_to = self.standard_ipaddr(access.get('access_to'))
                access_level = access.get('access_level')

                if access_level not in common_constants.ACCESS_LEVELS:
                    err_msg = _('Unsupported level of access was provided - {0}'.format(access_level))
                    raise exception.InvalidShareAccess(reason=err_msg)

                task_id = self.client.allow_access_for_nfs(self.nfs_share_id, access_to, access_level)
                self.client.wait_task_until_complete(task_id, query_interval_seconds=0.5)

        elif action == 'deny':
            nfs_share_clients = {}
            result = self.client.get_nfs_share_clients({'nfs_share_id': self.nfs_share_id})
            for data in result:
                access_name = self.standard_ipaddr(data.get('name'))
                nfs_share_clients[access_name] = data.get('client_id_in_storage')

            for access in self.nfs_rules:
                access_to = self.standard_ipaddr(access.get('access_to'))
                if access_to in nfs_share_clients:
                    task_id = self.client.deny_access_for_nfs(
                        self.nfs_share_id, access_to, nfs_share_clients[access_to])
                    self.client.wait_task_until_complete(task_id, query_interval_seconds=0.5)
                else:
                    LOG.info(_("The access_to {0} does not exist").format(access_to))

    def _build_query_param(self):
        return {
            'storage_id': self.driver_config.storage_id,
            'zone_id': self.driver_config.zone_id if self.driver_config.zone_id else self.driver_config.storage_id,
            'vstore_raw_id': self.driver_config.vstore_raw_id,
            'name': 'share-' + self.share.get('share_id')
        }

    def _get_share_id(self):
        param = self._build_query_param()
        if not self.share_parent_id:
            file_system = self.client.query_specified_file_system(param)
            nfs_shares = self.client.get_nfs_share({'fs_id': file_system.get('id')})
            nfs_shares = [share for share in nfs_shares if not share.get('owning_dtree_id')]
        else:
            dtree = self.client.query_specified_dtree(param)
            nfs_shares = self.client.get_nfs_share({'owning_dtree_id': dtree.get('id')})

        if len(nfs_shares) > 1 or len(nfs_shares) == 0:
            err_msg = _("Expected at most 1 nfs share, but got {0}.").format(len(nfs_shares))
            raise exception.InvalidShare(reason=err_msg)

        self.nfs_share_id = nfs_shares[0].get('id')
