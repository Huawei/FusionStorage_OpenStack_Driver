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

"""Huawei Nas Driver for Huawei storage arrays."""

from oslo_config import cfg
from oslo_log import log

from manila import exception
from manila.i18n import _
from manila.share import driver

from .plugin.change_access import ChangeAccess
from .plugin.check_update_storage import CheckUpdateStorage
from .plugin.operate_share import OperateShare
from .plugin.operate_snapshot import OperateSnapShot
from .plugin.plugin_factory import PluginFactory
from .utils import constants

huawei_opts = [
    cfg.StrOpt(
        'manila_huawei_conf_file',
        default='/etc/manila/manila_huawei_conf.xml',
        help='The configuration file for the Manila Huawei driver.'
    ),
    cfg.StrOpt(
        'pool_qos_params',
        default='',
        help='Storage resource pool type and QoS coefficient'
    ),
    cfg.BoolOpt(
        'storage_ssl_two_way_auth',
        default=False,
        help='Whether to use mutual authentication.'
    ),
    cfg.StrOpt(
        'storage_ca_filepath',
        default='',
        help='CA certificate directory.'
    ),
    cfg.StrOpt(
        'storage_cert_filepath',
        default='',
        help='Client certificate directory.'
    ),
    cfg.StrOpt(
        'storage_key_filepath',
        default='',
        help='Client key directory.'
    )
]

CONF = cfg.CONF
CONF.register_opts(huawei_opts)
LOG = log.getLogger(__name__)


class HuaweiNasDriver(driver.ShareDriver):
    """Huawei Oceanstor Pacific Share Driver."""

    def __init__(self, *args, **kwargs):
        """Do initialization."""

        LOG.info("Enter into init function.")
        super(HuaweiNasDriver, self).__init__(False, *args, **kwargs)
        self.configuration = kwargs.get('configuration', None)
        self.ipv6_implemented = True
        self.storage_features = {}
        self.cluster_sn = None
        if self.configuration:
            self.configuration.append_config_values(huawei_opts)
            self.plugin_factory = PluginFactory(self.configuration,
                                                self._get_plugin_impl_type)
        else:
            err_msg = (_("Huawei configuration missing."))
            raise exception.InvalidShare(reason=err_msg)

    @staticmethod
    def _get_plugin_impl_type(backend_key, platform_key):
        if backend_key != constants.PRODUCT_PACIFIC:
            msg = _("Storage product must be %s.") % constants.PRODUCT_PACIFIC
            LOG.error(msg)
            raise exception.BadConfigurationException(reason=msg)
        platform_type = constants.PLATFORM_IMPL_MAPPING.get(platform_key)
        return constants.PLUGIN_COMMUNITY_IMPL, platform_type

    def check_for_setup_error(self):
        """Check for setup error."""

        LOG.info("********************Check conf file and plugin.********************")
        try:
            self.plugin_factory.instance_service(CheckUpdateStorage, None).check_service()
        except Exception as err:
            self.plugin_factory.disconnect_client()
            raise err

    def do_setup(self, context):
        """Initialize the huawei nas driver while starting."""

        LOG.info("********************Do setup the driver.********************")
        self.cluster_sn = self.plugin_factory.reset_client()

    def get_configured_ip_versions(self):
        return self.get_configured_ip_version()

    def get_configured_ip_version(self):
        return [4, 6] if self.ipv6_implemented else [4]

    def create_share(self, context, share, share_server=None):
        """Create a share."""

        LOG.info("********************Create a share.********************")
        location = self.plugin_factory.instance_service(
            OperateShare, share, self.storage_features, context).create_share()

        return location

    def delete_share(self, context, share, share_server=None):
        """Delete a share."""

        LOG.info("********************Delete a share.********************")
        self.plugin_factory.instance_service(
            OperateShare, share, self.storage_features, context).delete_share()

    def extend_share(self, share, new_size, share_server=None):
        """Extend a share."""

        LOG.info("********************Extend a share.********************")
        self.plugin_factory.instance_service(
            OperateShare, share, self.storage_features).change_share(new_size, 'extend')

    def shrink_share(self, share, new_size, share_server=None):
        """Shrink a share."""

        LOG.info("********************Shrink a share.********************")
        self.plugin_factory.instance_service(
            OperateShare, share, self.storage_features).change_share(new_size, 'shrink')

    def ensure_share(self, context, share, share_server=None):
        """Ensure the share is valid."""

        LOG.info("********************Ensure a share.********************")
        return self.plugin_factory.instance_service(
            OperateShare, share, self.storage_features, context).ensure_share()

    def allow_access(self, context, share, access, share_server=None):
        """Allow access to the share."""

        LOG.info("********************Allow access.********************")
        self.plugin_factory.instance_service(
            ChangeAccess, share, self.storage_features, context).allow_access(access)

    def deny_access(self, context, share, access, share_server=None):
        """Deny access to the share."""

        LOG.info("********************Deny access.********************")
        self.plugin_factory.instance_service(
            ChangeAccess, share, self.storage_features, context).deny_access(access)

    def update_access(self, context, share, access_rules,
                      add_rules=None, delete_rules=None, share_server=None):
        """Update access rules list."""

        LOG.info("********************Update access.********************")
        self.plugin_factory.instance_service(
            ChangeAccess, share, self.storage_features, context).update_access(
            access_rules, add_rules, delete_rules)

    def create_snapshot(self, context, snapshot, share_server=None):
        """Create snapshot to the share"""
        LOG.info("********************Create snapshot.********************")
        self.plugin_factory.instance_service(
            OperateSnapShot, snapshot, self.storage_features, context).create_snapshot()

    def delete_snapshot(self, context, snapshot, share_server=None):
        """Delete snapshot to the share"""
        LOG.info("********************Delete snapshot.********************")
        self.plugin_factory.instance_service(
            OperateSnapShot, snapshot, self.storage_features, context).delete_snapshot()

    def revert_to_snapshot(self, context, snapshot, share_access_rules, snapshot_access_rules, share_server=None):
        """Revert snapshot to the share"""
        LOG.info("********************Revert snapshot.********************")
        self.plugin_factory.instance_service(
            OperateSnapShot, snapshot, self.storage_features, context).revert_to_snapshot()

    def reload_qos(self, share, qos_vals=None):
        """create、update、delete share qos"""
        LOG.info("********************Reload share qos.********************")
        self.plugin_factory.instance_service(
            OperateShare, share, self.storage_features, is_use_platform=True).reload_qos(qos_vals)

    def get_pool(self, share):
        """Get the namespace of share's storage pool name"""
        LOG.info("********************Get share pool name.********************")
        return self.plugin_factory.instance_service(
            OperateShare, share, self.storage_features).get_pool()

    def _update_share_stats(self):
        """Retrieve status info from share group."""

        LOG.info("********************Update share stats.********************")
        backend_name = self.configuration.safe_get('share_backend_name')
        data = dict(
            share_backend_name=backend_name or 'OceanStorPacific_NFS_CIFS',
            vendor_name='Huawei',
            driver_version='2.7.2',
            storage_protocol='NFS_CIFS',
            driver_handles_share_servers=False,
            qos=True,
            snapshot_support=False,
            total_capacity_gb=0.0,
            free_capacity_gb=0.0,
            ipv6_support=True)
        try:
            self.plugin_factory.instance_service(
                CheckUpdateStorage, None).update_storage_pool(data)
        except Exception as err:
            self.plugin_factory.disconnect_client()
            raise err
        self._set_storage_features(data)
        super(HuaweiNasDriver, self)._update_share_stats(data)

    def _set_storage_features(self, storage_data):
        storage_pools = storage_data.get('pools')
        for pool_info in storage_pools:
            self.storage_features['sn'] = self.cluster_sn
            self.storage_features[pool_info.get('pool_name')] = pool_info
