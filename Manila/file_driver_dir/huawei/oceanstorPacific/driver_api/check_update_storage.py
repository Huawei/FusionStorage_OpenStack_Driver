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

from .base_share_property import BaseShareProperty
from ..helper import constants

LOG = log.getLogger(__name__)


class CheckUpdateStorage(BaseShareProperty):
    def __init__(self, helper, root):
        super(CheckUpdateStorage, self).__init__(helper, root=root)
        self.pools_free = {}
        self.free_pool = None

    def check_conf_file(self):

        resturl = self.root.findtext('Storage/RestURL')
        username = self.root.findtext('Storage/UserName')
        pwd = self.root.findtext('Storage/UserPassword')
        product = self.root.findtext('Storage/Product')
        pools = self.root.findtext('Filesystem/StoragePool')
        rsv_per = self.root.findtext('Storage/Reserved_percentage')
        max_rat = self.root.findtext('Storage/Max_over_subscription_ratio')

        if product == "Pacific":
            LOG.info(_("<Product> is Pacific."))
        else:
            err_msg = _("check_conf_file: Config file invalid. <Product> must be set to Pacific.")
            raise exception.InvalidInput(err_msg)

        if not (resturl and username and pwd):
            err_msg = _("check_conf_file: Config file invalid. <RestURL> <UserName> <UserPassword> must be set.")
            raise exception.InvalidInput(err_msg)

        if not pools or False in [i.strip().isdigit() for i in pools.split(';')]:
            err_msg = _("check_conf_file: Config file invalid. <StoragePool> id must be set and must be int.")
            raise exception.InvalidInput(err_msg)

        if rsv_per and not rsv_per.strip().isdigit():
            err_msg = _("check_conf_file: Config file invalid. <Reserved_percentage> must be int.")
            raise exception.InvalidInput(err_msg)

        if max_rat and not max_rat.strip().isdigit():
            err_msg = _("check_conf_file: Config file invalid. <Max_over_subscription_ratio> must be int.")
            raise exception.InvalidInput(err_msg)

    def check_service(self):

        storage_pools = self.root.findtext('Filesystem/StoragePool').strip()
        pools_list = list(map(int, storage_pools.split(',')))
        for pool_id in pools_list:
            result = self.helper.query_pool_by_id(pool_id)
            status_code = result['status']

            if status_code in constants.POOL_STATUS_OK:
                LOG.info(_("The storage pool(id:{0}) is healthy.".format(pool_id)))
            else:
                err_msg = _("The storage pool(id:{0}) is unhealthy.".format(pool_id))
                raise exception.InvalidHost(reason=err_msg)

        LOG.info(_('All the storage pools are healthy.'))

    def update_storage_pool(self, data, free_pool):

        self.free_pool = free_pool
        data["pools"] = []

        storage_pools = self.root.findtext('Filesystem/StoragePool').strip()
        pools_list = list(map(int, [i.strip() for i in storage_pools.split(';')]))
        tmp_rsv_per = self.root.findtext('Storage/Reserved_percentage')
        reserved_percentage = int(tmp_rsv_per.strip()) if tmp_rsv_per else 15
        tmp_max_rat = self.root.findtext('Storage/Max_over_subscription_ratio')
        max_over_subscription_ratio = int(tmp_max_rat.strip()) if tmp_max_rat else 1

        for pool_id in pools_list:
            result = self.helper.query_pool_by_id(pool_id)
            if result:
                total = round(float(result['totalCapacity']) / 1024, 1)
                allocated = round(float(result['allocatedCapacity']) / 1024, 1)
                used = round(float(result['usedCapacity']) / 1024, 1)
                free = round(float(total) - float(used), 2)
                provisioned = used
                pool = dict(
                    huawei_smartpartition=True,
                    huawei_smartcache=True,
                    pool_name=result['storagePoolName'],
                    qos=True,
                    compression=True,
                    provisioned_capacity_gb=provisioned,
                    allocated_capacity_gb=allocated,
                    free_capacity_gb=free,
                    total_capacity_gb=total,
                    reserved_percentage=reserved_percentage,
                    max_over_subscription_ratio=max_over_subscription_ratio,
                    dedupe=False,
                    thin_provisioning=True,
                    ipv6_support=True,
                    share_proto='DPC'
                )
                data["pools"].append(pool)

                self.pools_free[pool_id] = free
                if pool_id not in self.free_pool:
                    self.free_pool.append(pool_id)

        self.free_pool.sort(key=lambda x: self.pools_free.get(x), reverse=True)

        if data["pools"]:
            LOG.debug(_("Updated storage pools:{0} success".format(pools_list)))
        else:
            err_msg = (_("Update storage pools{0} fail.".format(pools_list)))
            raise exception.InvalidInput(reason=err_msg)
