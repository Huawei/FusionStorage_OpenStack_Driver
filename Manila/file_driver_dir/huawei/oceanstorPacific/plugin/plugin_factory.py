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
    def __init__(self, configuration, impl_type):
        self.config = configuration
        # 初始化配置文件
        self.driver_config = DriverConfig(self.config)
        self.impl_type = impl_type(self.config.product)

        # 实例化client
        self.client = self._get_client()

    def reset_client(self):
        self.client = self._get_client()
        self.client.login()

    def get_esn(self):
        if self.config.product == constants.PRODUCT_PACIFIC:
            return self.client.get_esn()
        return ''

    def instance_service(self, service_type, share,
                         storage_features=None, context=None):
        # 实例化service
        all_sub_class = self.get_sub_class(service_type)

        for sub_class in all_sub_class:
            if sub_class.get_impl_type() == self.impl_type:
                LOG.info("using impl: " + sub_class.__name__)
                return sub_class(self.client, share, self.config, context, storage_features)
        err_msg = (_("service_type: {0}, impl_type: {1} not found".format(service_type.__name__, self.impl_type)))
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

    def _get_client(self):
        product = self.config.product
        if product == constants.PRODUCT_PACIFIC:
            return PacificClient(self.config)

        if product == constants.PRODUCT_PACIFIC_GFS:
            return DMEClient(self.config)

        err_msg = (_("Init client for {0} error.".format(product)))
        LOG.info(err_msg)
        raise exception.InvalidInput(reason=err_msg)
