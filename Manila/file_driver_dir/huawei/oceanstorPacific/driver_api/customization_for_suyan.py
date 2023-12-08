# coding=utf-8
# Copyright (c) 2023 Huawei Technologies Co., Ltd.
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

from .operate_share import OperateShare
from .change_access import ChangeAccess
from .check_update_storage import CheckUpdateStorage
from ..helper import constants

LOG = log.getLogger(__name__)


class CustomizationOperate(OperateShare):
    def __init__(self, helper, share):
        super(CustomizationOperate, self).__init__(helper, share)
        self.share_parent_id = self.share.get('parent_share_id')
        self.dtree_name = None
        self.dtree_id = None

    def set_root(self, root):
        self.root = root
        return self

    def create_share(self, root, free_pool):
        if not self.share_parent_id:
            return super(CustomizationOperate, self).create_share(root, free_pool)

        self.root = root
        self.free_pool = free_pool
        self._check_domain()
        self._get_or_create_account()
        self._get_share_parent_info()
        self._create_dtree()
        self._create_dtree_quota()
        self._create_dtree_share_protocol()
        return self._get_dtree_location()

    def change_share(self, new_size, action):
        if not self.share_parent_id:
            return super(CustomizationOperate, self).change_share(new_size, action)

        if not self.share.get('export_locations') or not self.share.get('export_locations')[0].get('path'):
            err_msg = _("share fail for invalid export location.")
            raise exception.InvalidShare(reason=err_msg)

        namespace_info = self._get_dtree_namespace_info()
        self._get_dtree_info()
        self._get_quota_info(namespace_info, action, self.dtree_id,
                             new_size, constants.QUOTA_PARENT_TYPE_DTREE)
        self.helper.change_quota_size(self.quota_id, new_size)
        LOG.info("{0} share done. New size:{1}.".format(action, new_size))
        return True

    def update_qos(self, qos_specs, root):
        """苏研定制接口，根据传递的qos_specs，刷新share的qos信息，如果没有则创建对应qos"""

        self._get_update_qos_config(qos_specs)
        if not self.qos_config:
            err_msg = "Can not get qos config when update_qos, the qos_specs is {0}".format(qos_specs)
            LOG.error(err_msg)
            raise exception.InvalidShare(reason=err_msg)

        account_name = self._find_account_name(root)
        result = self.helper.query_account_by_name(account_name)
        self.account_id = result.get('id')
        self._get_parent_name_from_export_locations()

        qos_name = self.namespace_name
        qos_info = self.helper.query_qos_info_by_name(qos_name)
        if not qos_info.get("data"):
            self._create_qos_when_update_qos(qos_name)
        else:
            self._update_qos(qos_name)

    def parse_cmcc_qos_options(self):
        """苏研定制接口，查询share相关的qos 信息"""

        self._get_parent_name_from_export_locations()
        result = self.helper.query_qos_info_by_name(self.namespace_name)
        if not result.get("data"):
            return {}
        qos_info = result.get("data", {})

        share_qos_info = {
            "total_bytes_sec": qos_info.get("max_mbps", 0) * constants.BYTE_TO_MB,
            "total_iops_sec": qos_info.get("max_iops", 0)
        }
        return share_qos_info

    def get_share_usage(self, share_usages):
        """苏研定制接口，通过share_usages获取对应share的容量信息"""

        if not self.share.get('export_locations') or not self.share.get('export_locations')[0].get('path'):
            err_msg = _("Get namespace_name fail for invalid export location.")
            LOG.error(err_msg)
            raise exception.InvalidShare(reason=err_msg)

        self._get_parent_name_from_export_locations()

        return self._get_share_capacity(share_usages)

    def delete_share(self):
        if not self.share_parent_id:
            return super(CustomizationOperate, self).delete_share()

        if not self.share.get('export_locations') or not self.share.get('export_locations')[0].get('path'):
            LOG.warn(_("Delete share fail for invalid export location."))
            return False

        self._get_account_id()
        self._get_dtree_namespace_info()
        if not self._get_dtree_info():
            LOG.warn(_("Delete share fail, cannot find dtree info of share"))
            return False

        self._delete_dtree_share_protocol()
        self.helper.delete_dtree(self.dtree_name, self.namespace_name)
        return True

    def ensure_share(self):
        if not self.share_parent_id:
            return super(CustomizationOperate, self).ensure_share()

        if not self.share.get('export_locations') or not self.share.get('export_locations')[0].get('path'):
            err_msg = _(" share fail for invalid export location.")
            raise exception.InvalidShare(reason=err_msg)

        result = self._get_dtree_namespace_info()
        self._check_namespace_running_status(result)
        return self._get_ensure_share_location()

    def _get_max_band_width_qos_config(self, extra_specs):
        """
        苏研单独的qos 参数设置与读取，其支持的参数如下：
             “total_bytes_sec”：总吞吐量，单位Byte/s
             “total_iops_sec”： 总IOPS，单位个/s
        此处解析 max_band_width，从total_bytes_sec获取
        """
        # the total_bytes_sec is Byte/s the pacific need MB/s
        tmp_max_band_width = extra_specs.get('pacific:total_bytes_sec')
        if tmp_max_band_width is None:
            self.qos_config['max_band_width'] = constants.MAX_BAND_WIDTH
        elif (tmp_max_band_width.strip().isdigit()
                and 1 <= int(int(tmp_max_band_width.strip()) / constants.BYTE_TO_MB)
                      <= constants.BAND_WIDTH_UPPER_LIMIT):
            self.qos_config['max_band_width'] = int(int(tmp_max_band_width.strip()) / constants.BYTE_TO_MB)
        else:
            err_msg = _("The total_bytes_sec in share type "
                        "must be int([1, %s]).") % constants.BAND_WIDTH_UPPER_LIMIT
            raise exception.InvalidInput(reason=err_msg)

    def _get_max_iops_qos_config(self, extra_specs):
        """
        苏研单独的qos 参数设置与读取，其支持的参数如下：
             “total_bytes_sec”：总吞吐量，单位Byte/s
             “total_iops_sec”： 总IOPS，单位个/s
        此处解析 max_iops，从total_iops_sec获取
        """
        tmp_max_iops = extra_specs.get('pacific:total_iops_sec')
        if tmp_max_iops is None:
            self.qos_config['max_iops'] = constants.MAX_IOPS
        elif tmp_max_iops.strip().isdigit() \
                and 0 <= int(tmp_max_iops.strip()) <= constants.MAX_IOPS_UPPER_LIMIT:
            self.qos_config['max_iops'] = int(tmp_max_iops.strip())
        else:
            err_msg = _("The max_iops in share type "
                        "must be int([0, %s]).") % constants.MAX_IOPS_UPPER_LIMIT
            raise exception.InvalidInput(reason=err_msg)

    def _create_qos(self):
        qos_name = self.namespace_name
        try:
            result = self.helper.create_qos_for_suyan(qos_name, self.account_id, self.qos_config)
            qos_policy_id = result.get('id')
            self.helper.add_qos_association(self.namespace_name, qos_policy_id, self.account_id)
        except Exception as e:
            self._rollback_creat(2)
            raise e

    def _get_parent_name_from_export_locations(self):
        """二级目录场景下需要同时获取命名空间和dtree的名称"""

        export_locations = self.share.get('export_locations')[0].get('path')
        if self.share_parent_id:
            self.namespace_name = export_locations.split('\\')[-1].split('/')[-2]
        else:
            self.namespace_name = export_locations.split('\\')[-1].split('/')[-1]

    def _find_account_name(self, root=None):
        LOG.info("Get account name from xml.")
        if not root:
            err_msg = _("Can not get account name from config.")
            LOG.error(err_msg)
            raise exception.InvalidShare(reason=err_msg)
        account_name = root.findtext('Filesystem/AccountName').strip()
        if not account_name:
            err_msg = "Can not get account_name from xml, please check."
            LOG.error(err_msg)
            raise exception.InvalidShare(reason=err_msg)
        return account_name

    def _get_update_qos_config(self, qos_specs):
        tmp_max_band_width = str(qos_specs.get('total_bytes_sec'))
        if (tmp_max_band_width.strip().isdigit()
                and 1 <= int(int(tmp_max_band_width.strip()) / constants.BYTE_TO_MB)
                      <= constants.BAND_WIDTH_UPPER_LIMIT):
            self.qos_config['max_band_width'] = int(int(tmp_max_band_width.strip()) / constants.BYTE_TO_MB)

        tmp_max_iops = str(qos_specs.get('total_iops_sec'))
        if tmp_max_iops.strip().isdigit() \
                and 0 <= int(tmp_max_iops.strip()) <= constants.MAX_IOPS_UPPER_LIMIT:
            self.qos_config['max_iops'] = int(tmp_max_iops.strip())

    def _create_qos_when_update_qos(self, qos_name):
        try:
            result = self.helper.create_qos_for_suyan(qos_name, self.account_id, self.qos_config)
            qos_policy_id = result.get('id')
            self.helper.add_qos_association(self.namespace_name, qos_policy_id, self.account_id)
        except Exception as e:
            self.helper.delete_qos(self.namespace_name)
            raise e

    def _update_qos(self, qos_name):
        self.helper.change_qos_for_suyan(qos_name, self.account_id, self.qos_config)

    def _get_share_parent_info(self):
        """首先去存储查询父目录的命名空间信息，查询不到抛错"""

        self.namespace_name = 'share-' + self.share_parent_id
        namespace_info = self.helper.query_namespace_by_name(self.namespace_name)
        if namespace_info:
            LOG.info(_("Namespace({0}) found successfully.".format(self.namespace_name)))
            self.namespace_id = namespace_info.get('id')
            return

        err_msg = _('Create Dtree failed, Can not fount parent share info %s on storage.') % self.share_parent_id
        LOG.error(err_msg)
        raise exception.InvalidInput(reason=err_msg)

    def _create_namespace(self):
        """苏研定制接口创建namespace时直接使用share的ID作为命名空间的名称"""

        self.namespace_name = 'share-' + self.share.get('share_id')
        try:
            forbidden_dpc = constants.SUPPORT_DPC if 'DPC' in self.share_proto else constants.NOT_SUPPORT_DPC
            storage_pool_id = self.free_pool[0]
            result = self.helper.create_namespace(self.namespace_name, storage_pool_id, self.account_id,
                                                  forbidden_dpc, self.tier_info.get('atime_mode'))
            self.namespace_id = result.get('id')
        except Exception as e:
            self._rollback_creat(1)
            raise e

    def _create_dtree(self):
        """创建二级目录的dtree"""

        self.dtree_name = 'share-' + self.share.get('share_id')
        try:
            result = self.helper.create_dtree(self.dtree_name, self.namespace_name)
            self.dtree_id = result.get('id')
        except Exception as e:
            self._rollback_creat(1)
            raise e

    def _create_dtree_quota(self):
        """创建二级目录的配额"""

        quota_size = self.share.get('size')
        try:
            self.helper.creat_quota(self.dtree_id, quota_size,
                                    constants.QUOTA_PARENT_TYPE_DTREE)
        except Exception as e:
            self._rollback_creat(1)
            raise e

    def _create_dtree_share_protocol(self):
        """创建二级目录共享"""

        try:
            if 'NFS' in self.share_proto:
                self.helper.create_dtree_nfs_share(
                    self.namespace_name, self.dtree_name, self.account_id)
            if 'CIFS' in self.share_proto:
                self.helper.create_dtree_cifs_share(
                    self.namespace_name, self.dtree_name, self.account_id)
        except Exception as e:
            self._rollback_creat(2)
            raise e

    def _rollback_creat(self, level):
        """当创建dtree过程中出现error，需要将前面已创建的对象清理掉"""

        LOG.error(_("Try to rollback..."))
        if level >= 2:
            self._delete_dtree_share_protocol()
        if level >= 1:
            self.helper.delete_dtree(self.dtree_name, self.namespace_name)

        LOG.info(_("Rollback done."))

    def _delete_dtree_share_protocol(self):
        """
        二级目录场景下
        NFS根据dtree ID查询对应的共享信息
        CIFS根据dtree 名称查询对应的共享信息
        """

        if 'NFS' in self.share_proto:
            result = self.helper.query_nfs_share_information(self.account_id)
            for nfs_share in result:
                if str(self.dtree_id) == nfs_share.get('dtree_id'):
                    nfs_share_id = nfs_share.get('id')
                    self.helper.delete_nfs_share(nfs_share_id, self.account_id)
                    break
        if 'CIFS' in self.share_proto:
            result = self.helper.query_cifs_share_information(self.account_id)
            for cifs_share in result:
                if str(self.dtree_name) == cifs_share.get('name'):
                    cifs_share_id = cifs_share.get('id')
                    self.helper.delete_cifs_share(cifs_share_id, self.account_id)
                    break

    def _get_dtree_location(self):
        """返回二级目录的共享路径"""

        location = []
        share_path = self.namespace_name + '/' + self.dtree_name
        if 'NFS' in self.share_proto:
            location.append('NFS:' + self.domain + ":/" + share_path)
        if 'CIFS' in self.share_proto:
            location.append('CIFS:\\\\' + self.domain + '\\' + share_path)

        return location

    def _get_dtree_namespace_info(self):
        """二级目录场景下，通过location获取namespace名称后再去获取namespace信息"""

        self.export_locations = self.share.get('export_locations')[0].get('path')
        self.namespace_name = self.export_locations.split('\\')[-1].split('/')[-2]
        result = self.helper.query_namespace_by_name(self.namespace_name)
        self.namespace_id = result.get('id')
        return result

    def _get_dtree_info(self):
        """二级目录场景下，通过location获取dtree名称后再去获取dtree信息"""

        self.dtree_name = self.export_locations.split('\\')[-1].split('/')[-1]
        result = self.helper.query_dtree_by_name(self.dtree_name, self.namespace_id)
        for dtree_info in result:
            self.dtree_id = dtree_info.get('id')
            return True

        return False

    def _get_share_capacity(self, share_usages):
        namespace_info = share_usages.get(self.namespace_name)
        if not namespace_info:
            return {}

        return self._check_and_get_share_capacity(namespace_info)

    def _check_and_get_share_capacity(self, share_data):
        if share_data.get("name") != self.namespace_name:
            return {}

        if share_data.get("space_used") is None:
            err_msg = _("Can not get share data, the share data is {0}".format(share_data))
            LOG.error(err_msg)
            raise exception.InvalidShare(reason=err_msg)

        hard_limit = self.share.get("size")
        used_space = share_data.get("space_used") / constants.CAPACITY_UNIT_KB_TO_GB

        share_capacity = {
            "hard_limit": str(hard_limit),
            "used_space": str(used_space),
            "avail_space": str(hard_limit - used_space)
        }
        return share_capacity


class CustomizationChangeAccess(ChangeAccess):
    def __init__(self, helper, share, root):
        super(CustomizationChangeAccess, self).__init__(helper, share)
        self.root = root
        self.share_parent_id = self.share.get('parent_share_id')
        self.dtree_name = None

    def update_access(self, access_rules, add_rules, delete_rules):
        """如果传入的参数包含parent_share_id，则走二级目录的流程"""

        if not self.share_parent_id:
            return super(CustomizationChangeAccess, self).update_access(
                access_rules, add_rules, delete_rules)

        self._get_account_and_share_related_information()
        self._update_access_for_share(access_rules, add_rules, delete_rules)

        return True

    def allow_access(self, access):
        """如果传入的参数包含parent_share_id，则走二级目录的流程"""

        if not self.share_parent_id:
            return super(CustomizationChangeAccess, self).allow_access(access)

        self._get_account_and_share_related_information()
        self._classify_rules([access], 'allow')
        return True

    def deny_access(self, access):
        """如果传入的参数包含parent_share_id，则走二级目录的流程"""

        if not self.share_parent_id:
            return super(CustomizationChangeAccess, self).allow_access(access)

        self._get_account_and_share_related_information()
        self._classify_rules([access], 'deny')
        return True

    def _find_account_id(self):
        """通过xml文件配置的账户名称获取账户信息"""
        LOG.info("Find account id from xml, call by ChangeAccess")
        account_name = self.root.findtext('Filesystem/AccountName').strip()
        result = self.helper.query_account_by_name(account_name)
        self.account_id = result.get('id')

    def _get_account_and_share_related_information(self):
        """二级目录场景下，share_path需要包含dtree名称"""
        self._find_account_id()
        self._get_export_location_info()
        self._get_dtree_share_related_info()
        self._query_and_set_share_info()

    def _get_dtree_share_related_info(self):
        """二级目录场景下，需要获取命名空间和dtree的名称"""

        self.namespace_name = self.export_locations.split('\\')[-1].split('/')[-2]
        self.dtree_name = self.export_locations.split('\\')[-1].split('/')[-1]
        self.share_path = '/' + self.namespace_name + '/' + self.dtree_name


class CustomizationChangeCheckUpdateStorage(CheckUpdateStorage):
    def __init__(self, helper, root):
        super(CustomizationChangeCheckUpdateStorage, self).__init__(helper, root)
        self.account_id = None

    @staticmethod
    def _get_all_share_usages(all_namespace_info):
        """将所有的命名空间信息和其名称组成键值对"""

        all_share_usages = {}
        for namespace in all_namespace_info:
            all_share_usages[namespace.get('name')] = namespace

        return all_share_usages

    def get_all_share_usage(self):
        """苏研定制接口，获取对应帐户下所有的share信息"""

        self._find_account_id()
        all_namespace_info = self.helper.get_all_namespace_info(self.account_id)
        return self._get_all_share_usages(all_namespace_info)

    def _find_account_id(self):
        """获取账户信息"""

        account_name = self.root.findtext('Filesystem/AccountName').strip()
        result = self.helper.query_account_by_name(account_name)
        self.account_id = result.get('id')
