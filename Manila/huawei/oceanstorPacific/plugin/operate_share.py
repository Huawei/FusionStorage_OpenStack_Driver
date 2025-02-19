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

from abc import abstractmethod

from .base_plugin import BasePlugin


class OperateShare(BasePlugin):
    @abstractmethod
    def create_share(self):
        pass

    @abstractmethod
    def delete_share(self):
        pass

    @abstractmethod
    def ensure_share(self):
        pass

    @abstractmethod
    def change_share(self, new_size, action):
        pass

    @abstractmethod
    def get_share_usage(self, share_usages):
        pass

    @abstractmethod
    def update_qos(self, qos_specs):
        pass

    @abstractmethod
    def parse_cmcc_qos_options(self):
        pass

    @abstractmethod
    def show_qos(self):
        pass
