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
        name_locator = name_locator_info.get('once_migrate_policy_name_locator')
        migrate_policy = self.client.get_gfs_tier_migration_policies({
            'name_locator': name_locator
        })
        if len(migrate_policy) > 0:
            # 存在分级策略报错
            err_msg = _("migrate_policy {0} already exists".format(name_locator))
            raise exception.InvalidShare(reason=err_msg)
        # 不存在分级策略则启动一个
        if init_type == "Preheat":
            strategy = 'hot'
        elif init_type == "Precool":
            strategy = 'cold'
        else:
            err_msg = _("unknown init_type {0}".format(init_type))
            raise exception.InvalidShare(reason=err_msg)

        result = self.client.create_gfs_tier_migration_policy({
            'gfs_name_locator': name_locator_info.get('gfs_name_locator'),
            'name': name_locator_info.get('once_migrate_policy_name'),
            'migration_type': constants.DME_MIGRATE_ONCE,
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
        name_locator = name_locator_info.get('once_migrate_policy_name_locator')
        migrate_policy = self.client.get_gfs_tier_migration_policies({
            'name_locator': name_locator
        })
        if len(migrate_policy) <= 0:
            LOG.info(_("migrate_policy {0} not found".format(name_locator)))
            return {}
        policy = migrate_policy[0]
        return {
            "tier_status": self._dme_policy_status_to_enum_num(policy.get("policy_status")),
            "tier_process": policy.get("migration_percent"),
            "tier_type": self._dme_tier_grade_to_enum_suyan_str(policy.get("tier_grade")),
            "tier_path": policy.get("file_name_filter", {}).get("filter")
        }

    def terminate_share_tier(self):
        name_locator_info = self._combine_name_locator()
        name_locator = name_locator_info.get('once_migrate_policy_name_locator')
        migrate_policy = self.client.get_gfs_tier_migration_policies({
            'name_locator': name_locator
        })
        if not migrate_policy:
            LOG.info(_("migrate_policy {0} not found, skip delete".format(name_locator)))
            return True
        try:
            result = self.client.delete_gfs_tier_migration_policy({
                'name_locator': name_locator
            })
        except exception.ShareNotFound as err:
            LOG.warning("Tier policy not exist, no need to continue")
            return False

        try:
            self.client.wait_task_until_complete(result.get('task_id'))
        except Exception as err:
            LOG.error("Delete GFS tier migration policies task failed, reason is %s", err)
            raise err

        return True

    def modify_share_tier_policy(self, new_share):
        """
        1. create tier grade and tier migrate task in condition
        2. Enable Concurrent Tasks and wait until all tasks complete
        :param new_share: the target tier param
        :return: None
        """
        name_locator_info = self._combine_name_locator()
        task_id_list = []
        grade_task_id = self._handle_tier_grade_policy(name_locator_info, new_share)
        migrate_task_id = self._handle_tier_migrate_policy(name_locator_info, new_share)
        if grade_task_id:
            task_id_list.append(grade_task_id)
        if migrate_task_id:
            task_id_list.append(migrate_task_id)

        # Enable Concurrent Tasks and wait until all tasks complete
        try:
            self.concurrent_exec_waiting_tasks(task_id_list)
        except Exception as err:
            LOG.error("Task execute failed, reason is %s", err)
            raise err

    def _handle_tier_grade_policy(self, name_locator_info, new_share):
        """
        create tier grade task in condition
        :param name_locator_info: dict of all gfs name locator
        :param new_share: the target tier param
        :return: task id
        """
        result = {}
        name_locator_param = {
            'name_locator': name_locator_info.get('grade_policy_name_locator')
        }
        tier_grade_key = 'tier_grade'
        grade_policy_info = self.client.get_gfs_tier_grade_policies(name_locator_param)
        target_grade_level = new_share.get('share_tier_strategy', {}).get('tier_place')

        if not grade_policy_info and not target_grade_level:
            # 上层没下发策略，且存储上也不存在策略，什么都不做
            LOG.info("The tier_place of share_tier_strategy is empty, do nothing")
        elif not grade_policy_info and target_grade_level:
            # 上层下发了策略，存储上不存在策略，创建策略
            result = self.client.create_gfs_tier_grade_policy({
                'gfs_name_locator': name_locator_info.get('gfs_name_locator'),
                'name': name_locator_info.get('grade_policy_name'),
                tier_grade_key: target_grade_level
            })
            LOG.info("GFS tier grade policy create_task create successfully")
        elif grade_policy_info and not target_grade_level:
            # 上层没下发策略，存储上存在策略，移除分级放置策略
            try:
                result = self.client.delete_gfs_tier_grade_policy(name_locator_param)
                LOG.info("GFS tier grade policy delete_task create successfully")
            except exception.ShareNotFound:
                LOG.warning("Tier policy not exist, no need to continue delete")
                result = {}
        elif grade_policy_info[0].get(tier_grade_key) != target_grade_level:
            # 上层下发了策略，存储上存在策略，且放置策略不一致，修改策略
            name_locator_param[tier_grade_key] = target_grade_level
            result = self.client.modify_gfs_tier_grade_policy(name_locator_param)
            LOG.info("GFS tier grade policy modify_task create successfully")
        else:
            # 上层下发了策略，存储上存在策略，且放置策略一致，什么都不做
            LOG.info("The target tier grade is same as the one on storage, do nothing")
        return result.get('task_id')

    def _handle_tier_migrate_policy(self, name_locator_info, new_share):
        """
        create tier migrate task in condition
        :param name_locator_info: dict of all gfs name locator
        :param new_share: the target tier param
        :return: task id
        """
        result = {}
        name_locator_param = {
            'name_locator': name_locator_info.get('periodicity_migrate_policy_name_locator')
        }
        current_tier_types = self.storage_features.get(
            self.storage_pool_name, {}).get('support_tier_types', [])
        lowest_tier_type = self.get_lowest_tier_grade(current_tier_types)
        tier_migrate_policy_info = self.client.get_gfs_tier_migration_policies(
            name_locator_param)
        target_migrate_expiration = new_share.get(
            'share_tier_strategy', {}).get('tier_migrate_expiration')
        tier_param = {
            'tier_grade': lowest_tier_type,
            'atime_filter': {
                'atime_operator': constants.DME_ATIME_RATHER_THAN,
                'atime': int(target_migrate_expiration) if target_migrate_expiration else 0,
                'atime_unit': constants.HTIME_UNIT
            }
        }

        if not tier_migrate_policy_info and not target_migrate_expiration:
            # 上层没下发策略，且存储上也不存在策略，什么都不做
            LOG.info("The tier_migrate_expiration of share_tier_strategy is empty, "
                     "do nothing")
        elif not tier_migrate_policy_info and target_migrate_expiration:
            # 上层下发了策略，存储上不存在策略，创建策略
            create_param = {
                'gfs_name_locator': name_locator_info.get('gfs_name_locator'),
                'name': name_locator_info.get('periodicity_migrate_policy_name'),
                'migration_type': constants.DME_MIGRATE_PERIODIC
            }
            create_param.update(tier_param)
            result = self.client.create_gfs_tier_migration_policy(create_param)
            LOG.info("Create GFS tier migrate periodic policy successfully")
        elif tier_migrate_policy_info and not target_migrate_expiration:
            # 上层没下发策略，存储上存在策略，移除分级迁移策略
            try:
                result = self.client.delete_gfs_tier_migration_policy(name_locator_param)
                LOG.info("Delete GFS tier migrate periodic policy successfully")
            except exception.ShareNotFound:
                LOG.warning("Tier policy not exist, no need to continue delete")
                result = {}
        elif (tier_migrate_policy_info[0].get('tier_grade') != lowest_tier_type or
                tier_migrate_policy_info[0].get('atime_filter', {}).get('atime') !=
              int(target_migrate_expiration)):
            # 上层下发了策略，存储上存在策略，且迁移策略不一致，修改策略
            name_locator_param.update(tier_param)
            result = self.client.modify_gfs_tier_migrate_policy(name_locator_param)
            LOG.info("Modify GFS tier migrate policy successfully")
        else:
            # 上层下发了策略，存储上存在策略，且放置策略一致，什么都不做
            LOG.info("The target tier migrate policy is same as the one on storage, "
                     "do nothing")
        return result.get('task_id')

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
        """
        combine all gfs name and gfs tier name locator
        :return: dict of all name locator
        """
        self._get_storage_pool_name()
        if not self.share.get('export_locations') or not self.share.get('export_locations')[0].get('path'):
            err_msg = _("change share fail for invalid export location.")
            raise exception.InvalidShare(reason=err_msg)

        gfs_name = constants.SHARE_PREFIX + self.share.get('share_id')
        once_migrate_policy_name = gfs_name + constants.ONCE_MIGRATE_NAME
        periodicity_migrate_policy_name = gfs_name + constants.PERIODICITY_NAME
        grade_policy_name = gfs_name + constants.GRADE_NAME
        gfs_name_locator = '@'.join([gfs_name, self.storage_pool_name])
        once_migrate_policy_name_locator = '@'.join([once_migrate_policy_name, gfs_name_locator])
        periodicity_migrate_policy_name_locator = '@'.join([periodicity_migrate_policy_name, gfs_name_locator])
        grade_policy_name_locator = '@'.join([grade_policy_name, gfs_name_locator])
        return {
            'gfs_name': gfs_name,
            'once_migrate_policy_name': once_migrate_policy_name,
            'periodicity_migrate_policy_name': periodicity_migrate_policy_name,
            'grade_policy_name': grade_policy_name,
            'gfs_name_locator': gfs_name_locator,
            'once_migrate_policy_name_locator': once_migrate_policy_name_locator,
            'periodicity_migrate_policy_name_locator': periodicity_migrate_policy_name_locator,
            'grade_policy_name_locator': grade_policy_name_locator
        }
