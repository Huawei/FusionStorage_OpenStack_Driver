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

from oslo_log import log

from manila import exception
from manila.i18n import _

from . import driver_api, helper

LOG = log.getLogger(__name__)


class OceanStorPacificStorageConnection(object):
    """
    OceanStorPacificStorageConnection class for Huawei OceanStorPacific storage system.
    采用门面模式，由 connection类 统一提供 oceanstorPacific_nas.py所需要的接口调用。
    connection类会调用 driver_api实现接口，同时会持有一个root对象，helper对象和free_pool对象。
    其中root对象为xml配置文件，helper对象为Pacific提供的Restful接口，free_pool对象为当前可用存储池id。
    driver_api主要完成业务逻辑的处理，同时调用helper对象完成和Pacific的交互，也会读取or检查 root对象中的信息。
    """

    def __init__(self, root):
        self.root = root

        self.helper = None
        self.free_pool = []

    def check_conf_file(self):
        """检查配置项，确保配置项设置无误"""
        driver_api.CheckUpdateStorage(self.helper, self.root).check_conf_file()

    def check_service(self):
        """检查存储池健康状态"""
        driver_api.CheckUpdateStorage(self.helper, self.root).check_service()

    def connect(self):
        """尝试和Pacific建立连接，具体实现为初始化一个helper对象。"""
        if self.root:
            self.helper = helper.RestHelper(self.root)
        else:
            err_msg = _("Huawei Pacific configuration missing.")
            raise exception.InvalidInput(reason=err_msg)
        self.helper.log_in_pacific()

    def update_share_stats(self, data):
        """查询存储池可用容量，更新data，更新free_pool"""
        driver_api.CheckUpdateStorage(self.helper, self.root).update_storage_pool(data, self.free_pool)

    def create_share(self, context, share, share_server):
        """创建共享，同时返回挂载路径信息"""
        location = driver_api.OperateShare(self.helper, share, self.root).create_share(self.free_pool)
        return location

    def delete_share(self, context, share, share_server):
        """删除共享"""
        driver_api.OperateShare(self.helper, share, self.root).delete_share()

    def ensure_share(self, share, share_server):
        """检查共享状态，同时返回挂载路径信息"""
        location = driver_api.OperateShare(self.helper, share, self.root).ensure_share()
        return location

    def extend_share(self, share, new_size, share_server):
        """扩容共享"""
        driver_api.OperateShare(self.helper, share, self.root).change_share(new_size, 'extend')

    def shrink_share(self, share, new_size, share_server):
        """缩容共享"""
        driver_api.OperateShare(self.helper, share, self.root).change_share(new_size, 'shrink')

    def allow_access(self, share, access, share_server):
        """在共享上添加一条鉴权信息"""
        driver_api.ChangeAccess(self.helper, share, self.root).allow_access(access)

    def deny_access(self, share, access, share_server):
        """在共享上删除一条鉴权信息"""
        driver_api.ChangeAccess(self.helper, share, self.root).deny_access(access)

    def update_access(self, share, access_rules, add_rules, delete_rules, share_server):
        """更新共享的鉴权信息，主要用于批量添加/删除鉴权信息"""
        driver_api.ChangeAccess(self.helper, share, self.root).update_access(access_rules, add_rules, delete_rules)
