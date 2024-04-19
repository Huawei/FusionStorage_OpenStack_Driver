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

import netaddr
from oslo_log import log
from manila import exception
from manila.share import share_types
from manila.i18n import _

LOG = log.getLogger(__name__)


class BaseShareProperty(object):
    def __init__(self, helper, share=None, root=None):
        self.helper = helper
        self.share = share
        self.root = root
        self.account_id = None
        self.account_name = None
        self.share_proto = self._get_share_proto()

    @staticmethod
    def standard_ipaddr(access):
        """
        When the added client permission is an IP address,
        standardize it. Otherwise, do not process it.
        """
        try:
            format_ip = netaddr.IPAddress(access)
            access_to = str(format_ip.format(dialect=netaddr.ipv6_compact))
            return access_to
        except Exception:
            return access

    @staticmethod
    def is_ipv4_address(ip_address):
        try:
            if netaddr.IPAddress(ip_address).version == 4:
                return True
            return False
        except Exception:
            return False

    def _get_account_name(self):
        LOG.info("Get account name from xml.")
        if self.root is None:
            err_msg = _("Can not get account name from config.")
            LOG.error(err_msg)
            raise exception.InvalidShare(reason=err_msg)
        account_name = self.root.findtext('Filesystem/AccountName').strip()
        if not account_name:
            err_msg = "Can not get account_name from xml, please check."
            LOG.error(err_msg)
            raise exception.InvalidShare(reason=err_msg)
        return account_name

    def _get_account_id(self):
        self.account_name = self._get_account_name()
        result = self.helper.query_account_by_name(self.account_name)
        self.account_id = result.get('id')

    def _get_share_proto(self):
        share_proto = []
        if self.share is None:
            return share_proto

        type_id = self.share.get('share_type_id')
        extra_specs = share_types.get_share_type_extra_specs(type_id)
        tmp_share_proto = extra_specs.get('share_proto', '').split('&')

        if 'DPC' in tmp_share_proto:
            share_proto.append('DPC')
            return share_proto

        return self.share.get('share_proto', '').split('&')

