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

from ..share_tier import ShareTier
from ...utils import constants

LOG = log.getLogger(__name__)


class SuyanGfsShareTier(ShareTier):

    def __init__(self, client, share=None, driver_config=None,
                 context=None, storage_features=None):
        super(SuyanGfsShareTier, self).__init__(
            client, share, driver_config, context, storage_features)
        self.share_parent_id = self.share.get('parent_share_id')
        self.dtree_name = None
        self.dtree_id = None

    @staticmethod
    def get_impl_type():
        return constants.PLUGIN_SUYAN_GFS_IMPL

    def initialize_share_tier(self, file_path, init_type):
        name_locator_info = self._combine_name_locator()
        name_locator = name_locator_info.get('name_locator')
        migrate_policy = self.client.get_tier_migration_policies_by_name_locator(name_locator)
        if len(migrate_policy) > 0:
            # 存在分级策略报错
            err_msg = _("migrate_policy {0} already exists".format(name_locator))
            raise exception.InvalidShare(reason=err_msg)
        else:
            # 不存在分级策略则启动一个
            strategy = None
            if init_type == "Preheat":
                strategy = 'hot'
            elif init_type == "Precool":
                strategy = 'cold'
            else:
                err_msg = _("unknown init_type {0}".format(init_type))
                raise exception.InvalidShare(reason=err_msg)

            result = self.client.create_tier_migration_policie({
                'gfs_name_locator': name_locator,
                'name': name_locator_info.get('migrate_policy_name'),
                'migration_type': 'one_off',
                "tier_grade": strategy,
                'file_name_filter': {
                    'filter': file_path,
                    'operator': 'contain'
                }
            })
            try:
                self.client.wait_task_until_complete(result.get('task_id'))
            except Exception as err:
                LOG.error("Create GFS tier migration policies task failed, reason is %s", err)
                raise err

    def get_share_tier_status(self):
        name_locator_info = self._combine_name_locator()
        name_locator = name_locator_info.get('name_locator')
        migrate_policy = self.client.get_tier_migration_policies_by_name_locator(name_locator)
        if len(migrate_policy) <= 0:
            LOG.info(_("migrate_policy {0} not found".format(name_locator)))
            return {}
        else:
            policy = migrate_policy[0]
            return {
                "tier_status": self._dme_policy_status_to_enum_num(policy.get("policy_status")),
                "tier_process": policy.get("migration_percent"),
                "tier_type": self._dme_tier_grade_to_enum_suyan_str(policy.get("tier_grade")),
                "tier_path": policy.get("file_name_filter", {}).get("filter")
            }

    def terminate_share_tier(self):
        name_locator_info = self._combine_name_locator()
        name_locator = name_locator_info.get('name_locator')
        migrate_policy = self.client.get_tier_migration_policies_by_name_locator(name_locator)
        if len(migrate_policy) > 0:
            result = self.client.delete_tier_migration_policie_by_name_locator(name_locator)
            try:
                self.client.wait_task_until_complete(result.get('task_id'))
            except Exception as err:
                LOG.error("Delete GFS tier migration policies task failed, reason is %s", err)
                raise err
        else:
            # 不存在分级策略记录日志
            LOG.info(_("migrate_policy {0} not found, skip delete".format(name_locator)))

    def _dme_policy_status_to_enum_num(self, status):
        status_map = {
            'failed': 1,
            'initializing': 2,
            'to_be_scheduled': 3,
            'running': 4,
            'complete': 5,
            'disabled': 6
        }
        res = status_map.get(status)
        if not res:
            err_msg = _("unknown policy_status {0}".format(status))
            raise exception.InvalidShare(reason=err_msg)
        return res

    def _dme_tier_grade_to_enum_suyan_str(self, tier_grade):
        tier_grade_map = {
            'hot': 'Preheat',
            'cold': 'Precool'
        }
        res = tier_grade_map.get(tier_grade)
        if not res:
            err_msg = _("unknown tier_grade {0}".format(tier_grade))
            raise exception.InvalidShare(reason=err_msg)
        return res

    def _combine_name_locator(self):
        self._get_storage_pool_name()
        if not self.share.get('export_locations') or not self.share.get('export_locations')[0].get('path'):
            err_msg = _("change share fail for invalid export location.")
            raise exception.InvalidShare(reason=err_msg)

        cluster_name = self.storage_pool_name
        gfs_name = constants.SHARE_PREFIX + self.share.get('share_id')
        migrate_policy_name = gfs_name + constants.ONCE_MIGRATE_NAME
        name_locator = '@'.join([cluster_name, gfs_name, migrate_policy_name])
        return {
            'cluster_name': cluster_name,
            'gfs_name': gfs_name,
            'migrate_policy_name': migrate_policy_name,
            'name_locator': name_locator
        }
