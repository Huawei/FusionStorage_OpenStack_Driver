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

"""Huawei Nas Driver for Huawei storage arrays."""

from xml.etree import ElementTree as ET
from oslo_config import cfg
from oslo_log import log
from oslo_utils import importutils

from manila import exception
from manila.i18n import _
from manila.share import driver

huawei_opts = [
    cfg.StrOpt('manila_huawei_conf_file',
               default='/etc/manila/manila_huawei_conf.xml',
               help='The configuration file for the Manila Huawei driver.')]

HUAWEI_UNIFIED_DRIVER_REGISTRY = {
    'Pacific': 'manila.share.drivers.huawei.oceanstorPacific.connection.OceanStorPacificStorageConnection'}

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
        if self.configuration:
            self.configuration.append_config_values(huawei_opts)
            backend_driver, root = self._get_backend_driver()
            self.plugin = importutils.import_object(backend_driver, root)
        else:
            err_msg = (_("Huawei configuration missing."))
            raise exception.InvalidShare(reason=err_msg)

    @staticmethod
    def _get_backend_driver_class(backend_key=None):

        backend_driver = HUAWEI_UNIFIED_DRIVER_REGISTRY.get(backend_key)
        if backend_driver is None:
            err_msg = (_("Product {0} is not supported. Product must be set to Pacific.".format(product)))
            raise exception.InvalidInput(reason=err_msg)
        return backend_driver

    def _get_backend_driver(self):

        filename = self.configuration.manila_huawei_conf_file
        try:
            tree = ET.parse(filename)
            root = tree.getroot()
            LOG.info(_("Read Huawei config file({0}) for Manila success.".format(filename)))
        except Exception:
            err_msg = (_("Read Huawei config file({0}) for Manila error.".format(filename)))
            raise exception.InvalidInput(reason=err_msg)

        product = root.findtext('Storage/Product').strip()
        if product is None:
            err_msg = (_("Can't find 'Storage/Product' in config file({0}).".format(filename)))
            raise exception.InvalidInput(reason=err_msg)

        backend_driver = self._get_backend_driver_class(product)
        return backend_driver, root

    def check_for_setup_error(self):
        """Check for setup error."""

        LOG.info("********************Check conf file and service.********************")
        self.plugin.check_conf_file()
        self.plugin.check_service()

    def do_setup(self, context):
        """Initialize the huawei nas driver while starting."""

        LOG.info("********************Do setup the driver.********************")
        self.plugin.connect()
        self.get_share_stats(True)

    def get_share_stats(self, refresh=False):
        """Get share status.
        If 'refresh' is True, run update the stats first.
        """

        LOG.debug("********************Update share stats.********************")
        if refresh:
            self._update_share_stats()
        return self._stats

    def _update_share_stats(self):
        """Retrieve status info from share group."""

        backend_name = self.configuration.safe_get('share_backend_name')
        data = dict(
            share_backend_name=backend_name or 'OceanStorPacific_NFS_CIFS',
            vendor_name='OceanStorPacific',
            driver_version='1.0',
            storage_protocol='NFS_CIFS',
            driver_handles_share_servers=False,
            qos=True,
            snapshot_support=False,
            total_capacity_gb=0.0,
            free_capacity_gb=0.0,
            ipv6_support=True)
        self.plugin.update_share_stats(data)
        super(HuaweiNasDriver, self)._update_share_stats(data)

    def get_configured_ip_versions(self):
        return self.get_configured_ip_version()

    def get_configured_ip_version(self):
        return [4, 6] if self.ipv6_implemented else [4]

    def create_share(self, context, share, share_server=None):
        """Create a share."""

        LOG.info("********************Create a share.********************")
        location = self.plugin.create_share(context, share, share_server)
        return location

    def delete_share(self, context, share, share_server=None):
        """Delete a share."""

        LOG.info("********************Delete a share.********************")
        self.plugin.delete_share(context, share, share_server)

    def extend_share(self, share, new_size, share_server=None):
        """Extend a share."""

        LOG.info("********************Extend a share.********************")
        self.plugin.extend_share(share, new_size, share_server)

    def shrink_share(self, share, new_size, share_server=None):
        """Shrink a share."""

        LOG.info("********************Shrink a share.********************")
        self.plugin.shrink_share(share, new_size, share_server)

    def ensure_share(self, context, share, share_server=None):
        """Ensure the share is valid."""

        LOG.info("********************Ensure a share.********************")
        return self.plugin.ensure_share(share, share_server)

    def allow_access(self, context, share, access, share_server=None):
        """Allow access to the share."""

        LOG.info("********************Allow access.********************")
        self.plugin.allow_access(share, access, share_server)

    def deny_access(self, context, share, access, share_server=None):
        """Deny access to the share."""

        LOG.info("********************Deny access.********************")
        self.plugin.deny_access(share, access, share_server)

    def update_access(self, context, share, access_rules, add_rules=None, delete_rules=None, share_server=None):
        """Update access rules list."""

        LOG.info("********************Update access.********************")
        self.plugin.update_access(share, access_rules, add_rules, delete_rules, share_server)
