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

import logging

from cinder.volume.drivers.fusionstorage import fs_utils

LOG = logging.getLogger(__name__)


class DriverForPlatform(object):
    """
    Cloud Platform Customization Class
    """
    def __init__(self, *args, **kwargs):
        super(DriverForPlatform, self).__init__(*args, **kwargs)

    def reload_qos(self, volume, qos_vals=None):
        """
        ZTE Cloud Platform Customization Interface
        QoS policies can be dynamically modified,remove,add
        and take effect on volumes in real time.
        """
        self._check_volume_exist_on_array(volume)
        volume_name = self._get_vol_name(volume)
        if not qos_vals:
            LOG.info("qos_vals is None, remove qos from volume %s", volume_name)
            self.fs_qos.remove(volume_name)
            return

        qos_vals = fs_utils.get_qos_param(qos_vals, self.client)
        vol_qos = self.client.get_qos_by_vol_name(volume_name)
        qos_name = vol_qos.get("qosName")
        if qos_name:
            LOG.info("volume already had qos, "
                     "update qos:%s of volume %s", qos_name, volume_name)
            self.client.modify_qos(qos_name, qos_vals)
            return

        LOG.info("volume did not have qos, "
                 "add qos to volume %s", volume_name)
        self.fs_qos.add(qos_vals, volume_name)
        return

    def modify_qos_with_volume(self, qos_id, volume):
        """
        Unified customization interface for Mobile
        network cloud to modify qos with volume
        """
        volume_name = self._get_vol_name(volume)
        vol_qos = self.client.get_qos_by_vol_name(volume_name)
        qos_name = vol_qos.get("qosName")
        if not qos_name:
            msg = ("dsware modify qos %(vol_qos)s with volume %(volume)s "
                   "failed! volume not associate qos") % {
                      'vol_qos': dict(vol_qos), 'volume': volume_name}
            self._raise_exception(msg)

        new_qos = fs_utils.get_qos_specs(qos_id, self.client)
        if not new_qos:
            msg = ("dsware modify qos %(qos_id)s with volume %(volume)s "
                   "failed! no valid qos specs found, consumer is front-end") % {
                      'qos_id': qos_id, 'volume': volume_name}
            self._raise_exception(msg)

        self.client.modify_qos(qos_name, new_qos)

    def rollback_snapshot(self, volume, snapshot):
        """
        Unified customization interface for
        Baidu Cloud Platform to rollback snapshot

        params:
        snapshot: Snapshot object to roll back
        volume: Volume object to be rolled back

        If a volume expand after a snapshot is created,
        the volume size is inconsistent with the snapshot size.
        In this case, snapshot rollback is not supported.
        """
        LOG.info("Begin to revert volume %s to snapshot %s", volume.id, snapshot.id)
        if snapshot.volume_size != volume.size:
            msg = "The volume size must be equal to the snapshot size."
            LOG.error(msg)
            self._raise_exception(msg)
        self.revert_to_snapshot(None, volume, snapshot)
