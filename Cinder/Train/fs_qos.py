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
import re
import time

from oslo_log import log as logging

from cinder import exception
from cinder.volume.drivers.fusionstorage import constants

LOG = logging.getLogger(__name__)


class FusionStorageQoS(object):
    def __init__(self, client):
        self.client = client

    @staticmethod
    def _is_openstack_qos_name(qos_name, vol_name):
        """
        When querying volume QoS info by volume name,
        if the volume does not have an associated QoS policy
        but the storage pool which the volume belongs have an associated QoS,
        the QoS of that storage pool will be retrieved.
        Since the information returned by the storage cannot identify
        whether this QoS is a storage pool QoS, Driver cannot determine
        whether this QoS is associated with current volume.
        As a result, when Driver attempts to disassociate the volume from the QoS,
        an error will be reported.
        So, Driver check the qos name whether create by openstack when update and delete qos
        """
        if len(vol_name) >= constants.QOS_MAX_INTERCEPT_LENGTH:
            qos_suffix = vol_name[-constants.QOS_MAX_INTERCEPT_LENGTH::]
        else:
            qos_suffix = vol_name
        qos_name_pattern = '^%s.*%s' % (constants.QOS_PREFIX, qos_suffix)
        if re.search(qos_name_pattern, qos_name):
            return True
        else:
            return False

    def add(self, qos, vol_name):
        localtime = time.strftime('%Y%m%d%H%M%S', time.localtime())
        # QoS policy name. The value contains 1 to 63 characters.
        # So we intercept volume_name Ensure that the length does not exceed 63
        vol_str = vol_name[-constants.QOS_MAX_INTERCEPT_LENGTH::] \
            if len(vol_name) >= constants.QOS_MAX_INTERCEPT_LENGTH else vol_name
        qos_name = constants.QOS_PREFIX + localtime + '_' + vol_str
        self.client.create_qos(qos_name, qos)
        try:
            self.client.associate_qos_with_volume(vol_name, qos_name)
        except exception.VolumeBackendAPIException:
            self.remove(vol_name)
            raise

    def _is_qos_associate_to_volume(self, qos_name):
        all_pools = self.client.query_storage_pool_info()
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
        if not qos_name:
            return

        if self._is_openstack_qos_name(qos_name, vol_name):
            self.client.disassociate_qos_with_volume(vol_name, qos_name)

            if not self._is_qos_associate_to_volume(qos_name):
                self.client.delete_qos(qos_name)
        else:
            LOG.warning("The QoS:%s is not created by OpenStack, ignore it", qos_name)

    def update(self, qos, vol_name):
        vol_qos = self.client.get_qos_by_vol_name(vol_name)
        qos_name = vol_qos.get("qosName")
        if not qos_name:
            return
        if self._is_openstack_qos_name(qos_name, vol_name):
            self.client.modify_qos(qos_name, qos)
        else:
            LOG.warning("The QoS:%s is not created by OpenStack, ignore it", qos_name)
