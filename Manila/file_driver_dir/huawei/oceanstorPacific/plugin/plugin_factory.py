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

from ..client.pacific_client import PacificClient
from ..client.dme_client import DMEClient
from ..utils.driver_config import DriverConfig
from ..utils import constants

LOG = log.getLogger(__name__)


class PluginFactory(object):
    def __init__(self, configuration, impl_func):
        self.config = configuration
        # 初始化配置文件
        self.driver_config = DriverConfig(self.config)
        self.impl_func = impl_func
        self.impl_type = None
        self.platform_type = None
        self.client = None

    def reset_client(self):
        # 配置文件校验
        self.driver_config.update_configs()
        # 实例化client
        self.client = self._get_client()
        self.impl_type, self.platform_type = self.impl_func(
            self.config.product, self.config.platform)
        system_esn = self.client.login().get('system_esn')
        self._set_config_after_login()
        return system_esn

    def disconnect_client(self):
        LOG.info("Begin to disconnect client")
        self.client.logout()

    def instance_service(self, service_type, share,
                         storage_features=None, context=None, is_use_platform=False):
        # 实例化service
        all_sub_class = self.get_sub_class(service_type)

        impl_type = self.platform_type if is_use_platform else self.impl_type

        for sub_class in all_sub_class:
            if impl_type in sub_class.get_impl_type():
                LOG.info("using impl: " + sub_class.__name__)
                return sub_class(self.client, share, self.config, context, storage_features)
        err_msg = (_("service_type: {0}, impl_type: {1} not found".format(
            service_type.__name__, self.impl_type)))
        raise exception.InvalidInput(reason=err_msg)

    def get_sub_class(self, service_type):
        all_sub_class = []
        self.recursive_get_sub_class(service_type, all_sub_class)
        return all_sub_class

    def recursive_get_sub_class(self, service_type, result):
        sub_classes = service_type.__subclasses__()
        if not sub_classes:
            pass
        for sub_class in sub_classes:
            if sub_class not in result:
                result.append(sub_class)
                self.recursive_get_sub_class(sub_class, result)

    def _set_config_after_login(self):
        if self.impl_type and self.impl_type == constants.PLUGIN_DME_FILESYSTEM_IMPL:
            LOG.info("********************set config after login********************")
            storage_id_by_sn = self._get_storage_id_by_sn()
            zone_id = self._get_zone_id_by_storage_id(storage_id_by_sn)
            vstore_id = self._get_vstore_id(storage_id_by_sn, zone_id)
            self._get_dpc_user_id_by_name(storage_id_by_sn, vstore_id, zone_id)

    def _get_dpc_user_id_by_name(self, storage_id_by_sn, vstore_id, zone_id):
        dpc_user = self.driver_config.config.dpc_user
        if not dpc_user:
            return
        param = {
            "storage_id": storage_id_by_sn,
            "vstore_id": vstore_id,
            "name": dpc_user
        }
        if zone_id:
            param["zone_id"] = zone_id
        users = self.client.get_dpc_administrators(param)
        filtered_user = next((user for user in users if user.get('name') == dpc_user), None)
        if filtered_user:
            user_id = filtered_user.get('id')
            setattr(self.driver_config.config, 'dpc_user_id', user_id)
            return
        raise ValueError("Failed get dpc user by storage_id:%s ,vstore_id:%s, and name:%s", storage_id_by_sn,
                         vstore_id, dpc_user)

    def _get_vstore_id(self, storage_id_by_sn, zone_id):
        vstore_raw_id = self.driver_config.config.vstore_raw_id
        param = {
            "storage_id": storage_id_by_sn,
            "raw_id": vstore_raw_id
        }
        if zone_id:
            param["zone_id"] = zone_id
        vstores = self.client.get_vstores(param)
        if vstores and len(vstores) == 1:
            vstore_id = vstores[0].get("id")
            setattr(self.driver_config.config, 'vstore_id', vstore_id)
            LOG.info("success get vstore_id:%s by storage id:%s and raw id:%s", vstore_id, storage_id_by_sn,
                     vstore_raw_id)
            return vstore_id
        raise ValueError("Failed get vstore_id by storage id:%s and raw id:%s", storage_id_by_sn, vstore_raw_id)

    def _get_zone_id_by_storage_id(self, storage_id_by_sn):
        zone_raw_id = self.driver_config.config.zone_raw_id
        if storage_id_by_sn and zone_raw_id:
            zones = self.client.get_cluster_zones(storage_id_by_sn)
            filtered_zone = next((zone for zone in zones if zone.get('zone_raw_id') == zone_raw_id), None)
            if filtered_zone:
                zone_native_id = filtered_zone.get('native_id')
                setattr(self.driver_config.config, 'zone_id', zone_native_id)
                LOG.info("success get zone:%s by storage id:%s and zone id:%s", zone_native_id, storage_id_by_sn,
                         zone_raw_id)
                return zone_native_id
            # 配置了zone_raw_id但是未查询到对应的zone,抛异常
            raise ValueError("Failed get zone by storage id:%s and zone id:%s", storage_id_by_sn, zone_raw_id)
        setattr(self.driver_config.config, 'zone_id', '')
        return None

    def _get_storage_id_by_sn(self):
        storage_sn = self.driver_config.config.storage_sn
        storages = self.client.get_storages(None)
        filtered_storage = next((storage for storage in storages if storage.get('sn') == storage_sn), None)
        if filtered_storage:
            storage_id = filtered_storage.get('id')
            setattr(self.driver_config.config, 'storage_id', storage_id)
            LOG.info("success get storage id:%s by sn:%s", storage_id, storage_sn)
            return storage_id
        raise ValueError("Failed get storage id by sn: %s" % storage_sn)

    def _get_client(self):
        product = self.config.product
        if product == constants.PRODUCT_PACIFIC:
            return PacificClient(self.config)

        if product == constants.PRODUCT_PACIFIC_GFS:
            return DMEClient(self.config)

        if product == constants.PRODUCT_DME_FILESYSTEM:
            return DMEClient(self.config)

        err_msg = (_("Init client for {0} error.".format(product)))
        LOG.info(err_msg)
        raise exception.InvalidInput(reason=err_msg)
