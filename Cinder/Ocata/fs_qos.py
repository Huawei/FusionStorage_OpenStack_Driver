# Copyright (c) 2019 Huawei Technologies Co., Ltd.
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

import time

from oslo_log import log as logging

from cinder import exception
from cinder.volume.drivers.fusionstorage import constants

LOG = logging.getLogger(__name__)


class FusionStorageQoS(object):
    def __init__(self, client):
        self.client = client

    def add(self, qos, vol_name):
        localtime = time.strftime('%Y%m%d%H%M%S', time.localtime())
        qos_name = constants.QOS_PREFIX + localtime
        self.client.create_qos(qos_name, qos)
        try:
            self.client.associate_qos_with_volume(vol_name, qos_name)
        except exception.VolumeBackendAPIException:
            self.remove(vol_name)
            raise

    def _is_qos_associate_to_volume(self, qos_name):
        all_pools = self.client.query_pool_info()
        volumes = None
        for pool in all_pools:
            volumes = self.client.get_qos_volume_info(
                pool.get('poolId'), qos_name)
            if volumes:
                break
        return volumes

    def remove(self, vol_name):
        vol_qos = self.client.get_qos_by_vol_name(vol_name)
        qos_name = vol_qos.get("qosName")
        if qos_name:
            self.client.disassociate_qos_with_volume(vol_name, qos_name)

            if not self._is_qos_associate_to_volume(qos_name):
                self.client.delete_qos(qos_name)

    def update(self, qos, vol_name):
        vol_qos = self.client.get_qos_by_vol_name(vol_name)
        qos_name = vol_qos.get("qosName")
        if qos_name:
            self.client.modify_qos(qos_name, qos)
