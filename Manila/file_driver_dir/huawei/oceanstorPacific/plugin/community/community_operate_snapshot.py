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

from ..operate_snapshot import OperateSnapShot
from ...utils import constants, driver_utils

LOG = log.getLogger(__name__)


class CommunityOperateSnapshot(OperateSnapShot):

    def __init__(self, client, snapshot, driver_config=None,
                 context=None, storage_features=None):
        self.client = client
        self.snapshot = snapshot
        self.context = context
        self.storage_features = storage_features
        self.driver_config = driver_config
        self.namespace_name = None
        self.namespace_id = None
        self.snapshot_name = None

    @staticmethod
    def get_impl_type():
        return constants.PLUGIN_COMMUNITY_IMPL

    def create_snapshot(self):
        LOG.info("Begin to create snapshot to share.")
        self._get_snapshot_params()
        self._get_namespace_id()
        self._create_snapshot()
        LOG.info("End to create snapshot to share.")

    def delete_snapshot(self):
        """Delete snapshot to the share"""
        LOG.info("Begin to delete snapshot to share.")
        self._get_snapshot_params()
        self._get_namespace_id()
        self._delete_snapshot()
        LOG.info("End to delete snapshot to share.")

    def revert_to_snapshot(self):
        """Revert snapshot to the share"""
        LOG.info("Begin to revert snapshot to share.")
        self._get_snapshot_params()
        self._get_namespace_id()
        self._revert_to_snapshot()
        LOG.info("End to revert snapshot to share.")

    def _get_snapshot_params(self):
        """Get snapshot's share params info"""
        self.namespace_name = "share-" + self.snapshot.get("share").get("share_id")
        self.snapshot_name = "snapshot_" + self.snapshot.get("snapshot_id").replace('-', '_')

    def _get_namespace_id(self):
        """Check snapshot's share whether exist in storage"""
        namespace_info = self.client.query_namespace_by_name(self.namespace_name)
        self.namespace_id = namespace_info.get("id")

    def _create_snapshot(self):
        """Create snapshot to the share"""
        if self.namespace_id is None:
            err_msg = _("Namespace does not exist, create snapshot failed")
            raise exception.InvalidShare(reason=err_msg)
        create_snapshot_param = {
            "name": self.snapshot_name,
            "namespace_name": self.namespace_name
        }
        self.client.create_snapshot(create_snapshot_param)

    def _delete_snapshot(self):
        """Delete snapshot to the share"""
        if self.namespace_id is None:
            LOG.warning(_("Cannot find namespace info of snapshot"))
            return
        query_and_delete_snapshot_param = {
            "name": self.snapshot_name,
            "namespace_name": self.namespace_name
        }
        snapshot_info = self.client.query_snapshot_info(query_and_delete_snapshot_param)
        if snapshot_info.get("id") is None:
            return
        self.client.delete_snapshot(query_and_delete_snapshot_param)

    def _revert_to_snapshot(self):
        """Revert snapshot to the share"""
        if self.namespace_id is None:
            err_msg = _("Namespace does not exist, revert snapshot failed")
            raise exception.InvalidShare(reason=err_msg)
        snapshot_rollback_status = self._check_snapshot_status()
        if not snapshot_rollback_status:
            self._wait_rollback_snapshot_complete()
            return
        rollback_snapshot_param = {
            "name": self.snapshot_name,
            "namespace_id": self.namespace_id,
            "rollback_speed": self.driver_config.rollback_rate
        }
        self.client.rollback_snapshot(rollback_snapshot_param)
        self._wait_rollback_snapshot_complete()

    def _wait_rollback_snapshot_complete(self, time_out_seconds=constants.DEFAULT_WAIT_TIMEOUT,
                                         query_interval_seconds=constants.SNAPSHOT_ROLLBACK_WAIT_INTERVAL):
        def check_snapshot_status_callback():
            return self._check_snapshot_status()
        driver_utils.wait_for_condition(check_snapshot_status_callback, query_interval_seconds, time_out_seconds)

    def _check_snapshot_status(self):
        """check snapshot status whether in rollback"""
        query_snapshot_param = {
            "name": self.snapshot_name,
            "namespace_name": self.namespace_name
        }
        snapshot_info = self.client.query_snapshot_info(query_snapshot_param)
        snapshot_id = snapshot_info.get("id")
        if snapshot_id is None:
            err_msg = _("Snapshot does not exist")
            raise exception.InvalidShare(reason=err_msg)
        if str(snapshot_info.get("status", "")) == constants.SNAPSHOT_ROLLBACKING_STATUS:
            LOG.info(_("Snapshot status is rollbacking"))
            return False
        return True
