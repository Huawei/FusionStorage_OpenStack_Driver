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

from ..community.community_change_access import CommunityChangeAccess
from ...utils import constants

LOG = log.getLogger(__name__)


class SuyanSingleChangeAccess(CommunityChangeAccess):
    def __init__(self, client, share=None, driver_config=None,
                 context=None, storage_features=None):
        super(SuyanSingleChangeAccess, self).__init__(
            client, share, driver_config, context, storage_features)
        self.share_parent_id = self._get_share_parent_id()
        self.dtree_name = None
        self.dtree_id = None

    @staticmethod
    def get_impl_type():
        return constants.PLUGIN_SUYAN_SINGLE_IMPL

    def update_access(self, access_rules, add_rules, delete_rules):
        """如果传入的参数包含parent_share_id，则走二级目录的流程"""

        if not self.share_parent_id:
            super(SuyanSingleChangeAccess, self).update_access(
                access_rules, add_rules, delete_rules)
            return

        if add_rules:
            self._get_share_access_proto(add_rules, True)
        if delete_rules:
            self._get_share_access_proto(delete_rules, False)
        if not add_rules and not delete_rules:
            self._get_share_access_proto(access_rules, True)
        self._get_account_and_share_related_information()
        self._update_access_for_share(add_rules, delete_rules)
        return

    def allow_access(self, access):
        """如果传入的参数包含parent_share_id，则走二级目录的流程"""

        if not self.share_parent_id:
            super(SuyanSingleChangeAccess, self).allow_access(access)
            return

        self._get_share_access_proto([access], True)
        self._get_account_and_share_related_information()
        self._classify_rules(self.allow_access_proto, 'allow')
        return

    def deny_access(self, access):
        """如果传入的参数包含parent_share_id，则走二级目录的流程"""

        if not self.share_parent_id:
            super(SuyanSingleChangeAccess, self).deny_access(access)
            return

        self._get_share_access_proto([access], False)
        self._get_account_and_share_related_information()
        self._classify_rules(self.deny_access_proto, 'deny')
        return

    def _get_account_and_share_related_information(self):
        """二级目录场景下，share_path需要包含dtree名称"""
        self._get_account_id()
        self._get_export_location_info()
        self._get_dtree_share_related_info()
        self._query_and_set_share_info(self.dtree_id, self.dtree_name)

    def _get_dtree_share_related_info(self):
        """二级目录场景下，需要获取命名空间和dtree的名称"""

        self.namespace_name = self.export_locations.split('\\')[-1].split('/')[-2]
        self.dtree_name = self.export_locations.split('\\')[-1].split('/')[-1]
        self.share_path = '/' + self.namespace_name + '/' + self.dtree_name
        namespace_info = self.client.query_namespace_by_name(
            self.namespace_name)
        self.namespace_id = namespace_info.get('id')
        dtree_info = self.client.query_dtree_by_name(
            self.dtree_name, self.namespace_id)
        for info in dtree_info:
            self.dtree_id = info.get('id')
