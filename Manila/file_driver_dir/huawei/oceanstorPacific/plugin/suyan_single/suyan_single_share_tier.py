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
import math

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

    @staticmethod
    def _get_tier_migrate_period_atime(migrate_period_policy):
        if not migrate_period_policy.get('atime'):
            return constants.TIER_MIGRATE_DEFAULT_ATIME
        atime = float(migrate_period_policy.get('atime'))
        atime_unit = migrate_period_policy.get('atime_unit', constants.HTIME_UNIT)
        if atime_unit == constants.HTIME_UNIT:
            return math.ceil(atime / constants.TIER_DAY_TO_HOUR)
        return atime

    def modify_share_tier_policy(self, new_share):
        # 查询文件系统
        self._get_account_id()
        self._get_storage_pool_name()
        namespace_name = 'share-' + self.share.get('share_id')
        namespace = self._query_namespace_by_name(namespace_name)
        namespace_id = namespace.get("id")

        # 处理分级放置策略
        self._handle_tier_grade_policy(namespace_name, namespace_id, new_share)

        # 处理分级迁移策略
        self._handle_tier_migrate_policy(namespace_name, namespace_id, new_share)

    def initialize_share_tier(self, file_path, init_type):
        """
        create preheat or precool task
        :param file_path:
        :param init_type:
        :return:
        """
        self._get_account_id()
        namespace_name = 'share-' + self.share.get('share_id')
        namespace = self._query_namespace_by_name(namespace_name)
        namespace_id = namespace.get("id")

        migrate_policy_name = namespace_name + constants.ONCE_MIGRATE_NAME
        migrate_policy = self._query_tier_migrate_policy_by_name_and_fs_id(
            migrate_policy_name, namespace_id)

        if migrate_policy:
            # 存在分级策略报错
            err_msg = _("migrate_policy {0} for fs {1} already exists".format(
                migrate_policy_name, namespace_name))
            raise exception.InvalidShare(reason=err_msg)

        # 不存在分级策略则创建
        create_param = {
            'name': migrate_policy_name,
            'fs_id': namespace_id,
            'account_id': self.account_id,
            'migration_type': constants.ONCE_MIGRATION_POLICY
        }
        self._set_tier_dtree_param(create_param, file_path, namespace_id)
        if init_type == constants.TIRE_TASK_PREHEAT:
            migrate_period_name = namespace_name + constants.PERIODICITY_NAME
            migrate_period_policy = self._query_tier_migrate_policy_by_name_and_fs_id(
                migrate_period_name, namespace_id)
            create_param.update({
                'strategy': constants.TIER_MIGRATE_STRATEGY_HOT,
                'expiration_to_cold': self._get_tier_migrate_period_atime(
                    migrate_period_policy)
            })
        elif init_type == constants.TIRE_TASK_PRECOOL:
            create_param.update({
                'strategy': constants.TIER_MIGRATE_STRATEGY_COLD
            })
        else:
            err_msg = _("unknown init_type {0}".format(init_type))
            raise exception.InvalidShare(reason=err_msg)

        result = self.client.create_tier_migrate_policy(create_param)
        return {'error_code': result.get('error_code')}

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
                "tier_process": migrate_policy.get("migration_percent"),
                "tier_type": self._pacific_tier_grade_to_enum_suyan_str(migrate_policy.get("strategy")),
                "tier_path": migrate_policy.get("path_name")
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

    def _pacific_tier_grade_to_enum_suyan_str(self, strategy):
        # 0：热，1：温，2：冷，3：迁到异构设备，4：异构设备取回。
        strategy_map = {
            0: 'Preheat',
            2: 'Precool'
        }
        res = strategy_map.get(strategy)
        if not res:
            err_msg = _("unknown strategy {0}".format(strategy))
            raise exception.InvalidShare(reason=err_msg)
        return res

    def _handle_tier_migrate_policy(self, namespace_name, namespace_id, new_share):
        # 按名称查询分级放置策略
        migrate_policy_name = namespace_name + constants.PERIODICITY_NAME
        migrate_policy = self._query_tier_migrate_policy_by_name_and_fs_id(
            migrate_policy_name, namespace_id)
        migrate_policy_id = migrate_policy.get('id')
        aim_tier_migrate_expiration = new_share.get(
            'share_tier_strategy', {}).get('tier_migrate_expiration')
        current_tier_types = self.storage_features.get(
            self.storage_pool_name, {}).get('support_tier_types', [])
        lowest_tier_migrate_level = driver_utils.convert_value_to_key(
            constants.TIER_ENUM, self.get_lowest_tier_grade(current_tier_types))

        if not migrate_policy and not aim_tier_migrate_expiration:
            # 上层没下发策略，且存储上也不存在策略，啥也不做
            LOG.info(_("The aim_tier_migrate_expiration is empty, do nothing"))
        elif not migrate_policy and aim_tier_migrate_expiration:
            # 上层下发了策略，存储上不存在策略，创建策略
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
            LOG.info("Create tier migrate policy successfully")
        elif migrate_policy and not aim_tier_migrate_expiration:
            # 上层没下发策略，存储上存在策略，移除分级迁移策略
            self.client.delete_tier_migrate_policy_by_id(migrate_policy_id, self.account_id)
            LOG.info("Delete tier migrate policy successfully")
        elif (str(migrate_policy.get('strategy')) != lowest_tier_migrate_level
              or int(migrate_policy.get('atime')) != int(aim_tier_migrate_expiration)):
            # 上层下发了策略，存储上存在策略，且放置策略不一致，修改策略
            self.client.modify_tier_migrate_policy_by_id(
                migrate_policy_id, lowest_tier_migrate_level,
                int(aim_tier_migrate_expiration), self.account_id)
            LOG.info("Modify tier migrate policy successfully")
        else:
            # 上层下发了策略，存储上存在策略，且放置策略一致，啥也不做
            LOG.info("The configured tier same as the one on storage, do nothing")

    def _handle_tier_grade_policy(self, namespace_name, namespace_id, new_share):
        # 按名称查询分级放置策略
        grade_policy_name = namespace_name + constants.GRADE_NAME
        grade_policy = self._query_tier_grade_policy_by_name_and_fs_id(grade_policy_name, namespace_id)
        grade_policy_id = grade_policy.get("id")

        aim_tier_grade_policy = new_share.get('share_tier_strategy', {}).get('tier_place')

        if not grade_policy and not aim_tier_grade_policy:
            # 上层没下发策略，且存储上也不存在策略，啥也不做
            LOG.info(_("The aim_tier_grade_policy is empty, do nothing"))
        elif not grade_policy and aim_tier_grade_policy:
            # 上层下发了策略，存储上不存在策略，创建策略
            self.client.create_tier_grade_policy({
                'name': grade_policy_name,
                'fs_id': namespace_id,
                'strategy': driver_utils.convert_value_to_key(
                    constants.TIER_ENUM, aim_tier_grade_policy),
                'account_id': self.account_id
            })
            LOG.info("Create tier grade policy successfully")
        elif grade_policy and not aim_tier_grade_policy:
            # 上层没下发策略，存储上存在策略，移除分级放置策略
            self.client.delete_tier_grade_policy_by_id(grade_policy_id, self.account_id)
            LOG.info("Delete tier grade policy successfully")
        elif constants.TIER_ENUM.get(str(grade_policy.get('strategy'))) != aim_tier_grade_policy:
            # 上层下发了策略，存储上存在策略，且放置策略不一致，修改策略
            self.client.modify_tier_grade_policy_by_id(
                grade_policy_id, driver_utils.convert_value_to_key(
                    constants.TIER_ENUM, aim_tier_grade_policy),
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

    def _set_tier_dtree_param(self, create_param, file_path, namespace_id):
        """
        check is file path include dtree dic,
        if true, param add 'dtree_id'
        :param create_param:
        :param file_path:
        :param namespace_id:
        :return:
        """
        path_name_key = 'path_name'
        if file_path == constants.PATH_SEPARATOR:
            create_param[path_name_key] = file_path
            return create_param
        if not file_path.startswith(constants.PATH_SEPARATOR):
            file_path = constants.PATH_SEPARATOR + file_path
        if not file_path.endswith(constants.PATH_SEPARATOR):
            file_path += constants.PATH_SEPARATOR
        path_name_list = file_path.split(constants.PATH_SEPARATOR)
        dtree_name = path_name_list[1]
        result = self.client.query_dtree_by_name(dtree_name, namespace_id)
        if not result:
            create_param[path_name_key] = file_path
            return create_param
        for dtree_info in result:
            dtree_id = dtree_info.get('id').split('@')[1]
            path_name_list.pop(1)
            create_param.update({
                'dtree_id': dtree_id,
                path_name_key: constants.PATH_SEPARATOR.join(path_name_list)
            })
            return create_param
