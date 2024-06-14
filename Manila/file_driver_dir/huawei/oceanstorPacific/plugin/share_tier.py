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


class ShareTier(BasePlugin):
    @abstractmethod
    def modify_share_tier_policy(self, new_share):
        pass

    @abstractmethod
    def initialize_share_tier(self, file_path, init_type):
        pass

    @abstractmethod
    def get_share_tier_status(self):
        pass

    @abstractmethod
    def terminate_share_tier(self):
        pass
