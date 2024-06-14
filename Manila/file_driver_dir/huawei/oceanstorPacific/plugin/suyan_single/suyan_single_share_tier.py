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
from ...utils import constants, driver_utils

LOG = log.getLogger(__name__)


class SuyanSingleShareTier(ShareTier):

    def __init__(self, client, share=None, driver_config=None,
                 context=None, storage_features=None):
        super(SuyanSingleShareTier, self).__init__(
            client, share, driver_config, context, storage_features)
        self.share_parent_id = self.share.get('parent_share_id')
        self.dtree_name = None
        self.dtree_id = None

    @staticmethod
    def get_impl_type():
        return constants.PLUGIN_SUYAN_SINGLE_IMPL

    def modify_share_tier_policy(self, new_share):
        # 查询文件系统
        self._get_account_id()
        namespace_name = 'share-' + self.share.get('share_id')
        namespace = self._query_namespace_by_name(namespace_name)
        namespace_id = namespace.get("id")

        # 处理分级放置策略
        self._handle_tier_grade_policy(namespace_name, namespace_id, new_share)

        # 处理分级迁移策略
        self._handle_tier_migrate_policy(namespace_name, namespace_id,
                                         namespace.get("storage_pool_id"), new_share)

    def initialize_share_tier(self, file_path, init_type):
        self._get_account_id()
        namespace_name = 'share-' + self.share.get('share_id')
        namespace = self._query_namespace_by_name(namespace_name)
        namespace_id = namespace.get("id")

        migrate_policy_name = namespace_name + constants.ONCE_MIGRATE_NAME
        migrate_policy = self._query_tier_migrate_policy_by_name_and_fs_id(migrate_policy_name, namespace_id)

        if migrate_policy:
            # 存在分级策略报错
            err_msg = _("migrate_policy {0} for fs {1} already exists".format(migrate_policy_name, namespace_name))
            raise exception.InvalidShare(reason=err_msg)
        else:
            # 不存在分级策略则启动一个
            # 0：热，1：温，2：冷
            strategy = None
            if init_type == "Preheat":
                strategy = 0
            elif init_type == "Precool":
                strategy = 2
            else:
                err_msg = _("unknown init_type {0}".format(init_type))
                raise exception.InvalidShare(reason=err_msg)

            self.client.create_tier_migrate_policy({
                'name': migrate_policy_name,
                'fs_id': namespace_id,
                'strategy': strategy,
                'path_name': file_path,
                'account_id': self.account_id,
                'migration_type': constants.ONCE_MIGRATION_POLICY
            })

    def get_share_tier_status(self):
        namespace_name = 'share-' + self.share.get('share_id')
        namespace = self._query_namespace_by_name(namespace_name)
        namespace_id = namespace.get("id")

        migrate_policy_name = namespace_name + constants.ONCE_MIGRATE_NAME
        migrate_policy = self._query_tier_migrate_policy_by_name_and_fs_id(migrate_policy_name, namespace_id)

        if not migrate_policy:
            # 不存在分级策略报错
            LOG.info(_("migrate_policy {0} for fs {1} not found"
                       .format(migrate_policy_name, namespace_name)))
            return {}
        else:
            return {
                "tier_status": migrate_policy.get("policy_status"),
                "tier_process": migrate_policy.get("migration_percent")
            }

    def terminate_share_tier(self):
        self._get_account_id()
        namespace_name = 'share-' + self.share.get('share_id')
        namespace = self._query_namespace_by_name(namespace_name)
        namespace_id = namespace.get("id")

        migrate_policy_name = namespace_name + constants.ONCE_MIGRATE_NAME
        migrate_policy = self._query_tier_migrate_policy_by_name_and_fs_id(migrate_policy_name, namespace_id)

        if not migrate_policy:
            # 不存在分级策略记录日志
            LOG.info(_("migrate_policy {0} for fs {1} not found, skip delete"
                       .format(migrate_policy_name, namespace_name)))
        else:
            migrate_policy_id = migrate_policy.get('id')
            self.client.delete_tier_migrate_policy_by_id(migrate_policy_id, self.account_id)

    def _handle_tier_migrate_policy(self, namespace_name, namespace_id, namespace_pool_id, new_share):
        # 找最小level
        aim_tier_migrate_expiration = new_share.get('tier_migrate_expiration')
        lowest_tier_migrate_level = None
        if aim_tier_migrate_expiration:
            disk_pool = self.client.query_disk_pool_by_storagepool_id(namespace_pool_id)
            pool_tier_type = set()
            for pool in disk_pool:
                pool_tier_type.add(str(pool.get('poolTier')))
            for level in constants.SORTED_DISK_POOL_TIER_LEVEL:
                if level in pool_tier_type:
                    level_str = constants.DISK_POOL_TIER_ENUM.get(level)
                    lowest_tier_migrate_level = driver_utils.convert_value_to_key(
                        constants.TIER_ENUM, level_str)
                    LOG.info(_("lowest_tier_migrate_level is {0}".format(lowest_tier_migrate_level)))
                    break
            if not lowest_tier_migrate_level:
                err_msg = _("Cannot find lowest_tier_migrate_level in {0}.".format(pool_tier_type))
                raise exception.InvalidShare(reason=err_msg)

        # 按名称查询分级放置策略
        migrate_policy_name = namespace_name + constants.PERIODICITY_NAME
        migrate_policy = self._query_tier_migrate_policy_by_name_and_fs_id(migrate_policy_name, namespace_id)

        # 新增、修改、删除
        if migrate_policy:
            # 存在策略，修改或删除
            migrate_policy_id = migrate_policy.get('id')
            if aim_tier_migrate_expiration:
                # 下发了修改参数，修改
                self.client.modify_tier_migrate_policy_by_id(
                    migrate_policy_id, lowest_tier_migrate_level,
                    int(aim_tier_migrate_expiration), self.account_id)
            else:
                # 未下发修改参数，删除
                self.client.delete_tier_migrate_policy_by_id(migrate_policy_id, self.account_id)
        else:
            # 不存在策略
            self.client.create_tier_migrate_policy({
                'name': migrate_policy_name,
                'fs_id': namespace_id,
                'strategy': lowest_tier_migrate_level,
                'account_id': self.account_id,
                'migration_type': constants.PERIODIC_MIGRATION_POLICY,
                'atime': int(aim_tier_migrate_expiration),
                'atime_unit': constants.HTIME_UNIT,
                'atime_operator': constants.MATCH_RULE_GT
            })

    def _handle_tier_grade_policy(self, namespace_name, namespace_id, new_share):
        # 按名称查询分级放置策略
        grade_policy_name = namespace_name + constants.GRADE_NAME
        grade_policy = self._query_tier_grade_policy_by_name_and_fs_id(grade_policy_name, namespace_id)
        grade_policy_id = grade_policy.get("id")

        aim_tier_grade_policy = new_share.get('tier_grade_policy', {})
        aim_tier_level = aim_tier_grade_policy.get('tier_place')

        if not grade_policy and not aim_tier_grade_policy:
            # 上层没下发策略，且存储上也不存在策略，啥也不做
            LOG.info(_("The aim_tier_grade_policy is empty, do nothing"))
        elif not grade_policy and aim_tier_grade_policy:
            # 上层下发了策略，存储上不存在策略，创建策略
            self.client.create_tier_grade_policy({
                'name': grade_policy_name,
                'fs_id': namespace_id,
                'strategy': driver_utils.convert_value_to_key(
                    constants.TIER_ENUM, aim_tier_level),
                'account_id': self.account_id
            })
            LOG.info("Create tier grade policy successfully")
        elif grade_policy and not aim_tier_grade_policy:
            # 上层没下发策略，存储上存在策略，移除分级放置策略
            self.client.delete_tier_grade_policy_by_id(grade_policy_id, self.account_id)
            LOG.info("Delete tier grade policy successfully")
        elif constants.TIER_ENUM.get(str(grade_policy.get('strategy'))) != aim_tier_level:
            # 上层下发了策略，存储上存在策略，且放置策略不一致，修改策略
            self.client.modify_tier_grade_policy_by_id(
                grade_policy_id, driver_utils.convert_value_to_key(
                    constants.TIER_ENUM, aim_tier_level),
                self.account_id)
            LOG.info("Modify tier grade policy successfully")
        else:
            # 上层下发了策略，存储上存在策略，且放置策略一致，啥也不做
            LOG.info("The configured tier same as the one on storage, do nothing")

    def _query_namespace_by_name(self, namespace_name):
        namespace = self.client.query_namespace_by_name(namespace_name)
        if not namespace:
            err_msg = _("Namespace {0} does not exist.".format(namespace_name))
            raise exception.InvalidShare(reason=err_msg)
        return namespace

    def _query_tier_migrate_policy_by_name_and_fs_id(self, tier_migrate_policy_name, fs_id):
        result = self.client.query_tier_migrate_policies_by_name(tier_migrate_policy_name)
        for each in result:
            if each.get('name') == tier_migrate_policy_name and fs_id == each.get('fs_id'):
                return each
        return {}

    def _query_tier_grade_policy_by_name_and_fs_id(self, tier_grade_policy_name, fs_id):
        result = self.client.query_tier_grade_policies_by_name(tier_grade_policy_name)
        for each in result:
            if each.get('name') == tier_grade_policy_name and fs_id == each.get('fs_id'):
                return each
        return {}
