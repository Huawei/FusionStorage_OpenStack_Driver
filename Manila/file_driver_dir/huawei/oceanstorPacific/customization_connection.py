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

from . import driver_api
from .connection import OceanStorPacificStorageConnection

LOG = log.getLogger(__name__)


class OceanStorPacificStorageConnectionForSuyan(OceanStorPacificStorageConnection):
    """
    OceanStorPacificStorageConnectionForSuyan继承OceanStorPacificStorageConnection
    添加上对接苏研云平台需要的定制接口
    """

    def create_share(self, context, share, share_server):
        """苏研qos为定制的参数，所以此处重写qos相关解析方法"""
        location = driver_api.CustomizationOperate(self.helper, share).create_share(self.root, self.free_pool)
        return location

    def delete_share(self, context, share, share_server):
        """删除share接口，苏研配置单独的一个账户用于创建ip"""
        driver_api.CustomizationOperate(self.helper, share).set_root(self.root).delete_share()

    def allow_access(self, share, access, share_server):
        """在共享上添加一条鉴权信息，苏研配置单独的一个账户用于创建ip"""
        driver_api.CustomizationChangeAccess(self.helper, share, self.root).allow_access(access)

    def deny_access(self, share, access, share_server):
        """在共享上删除一条鉴权信息，苏研配置单独的一个账户用于创建ip"""
        driver_api.CustomizationChangeAccess(self.helper, share, self.root).deny_access(access)

    def update_access(self, share, access_rules, add_rules, delete_rules, share_server):
        """更新共享的鉴权信息，主要用于批量添加/删除鉴权信息，苏研配置单独的一个账户用于创建ip"""
        driver_api.CustomizationChangeAccess(self.helper, share, self.root).update_access(
            access_rules, add_rules, delete_rules)

    def get_all_share_usage(self):
        """苏研定制接口，获取所有的share信息 返回存储上所有的share"""
        all_share_usages = driver_api.CustomizationChangeCheckUpdateStorage(
            self.helper, self.root).get_all_share_usage()
        return all_share_usages

    def get_share_usage(self, share, share_usages):
        """
        苏研定制接口，通过get_all_share_usage查询返回的所有share信息，获取到需要的share容量信息
        :param share：Manila下发需要查询的返回的share信息
        :param share_usages： get_all_share_usage接口返回的存储上所有的share信息
        :return:share_capacity: dict结构，返回share的容量信息，需要的字段如下：
                      {
                       “hard_limit”： 总容量，
                       “avail_space”：可用容量，
                       “used_space”： 已使用容量
                       }
        """
        share_capacity = driver_api.CustomizationOperate(self.helper, share).get_share_usage(share_usages)
        return share_capacity

    def update_qos(self, share, qos_specs):
        """
        苏研定制接口，通过qos_specs中的qos信息，对share进行qos更新
        :param share：Manila下发需要查询的返回的share信息
        :param qos_specs： 需要更新的qos信息，其有效的入参如下：
            {
             “total_bytes_sec”：总吞吐量，
             “total_iops_sec”： 总IOPS，
            }
        :return: None
        """
        driver_api.CustomizationOperate(self.helper, share).update_qos(qos_specs, self.root)

    def parse_cmcc_qos_options(self, share):
        """
        苏研定制接口，返回share绑定的qos信息
        :param share: Manila下发需要查询的返回的share信息
        :return: share_qos_info: 查询到的qos信息，参数如下：
            {
             “total_bytes_sec”：总吞吐量，
             “total_iops_sec”： 总IOPS，
            }
        """
        share_qos_info = driver_api.CustomizationOperate(self.helper, share).parse_cmcc_qos_options()
        return share_qos_info
