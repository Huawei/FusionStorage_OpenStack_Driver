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

import os

from lxml import etree as ET
from oslo_log import log

from manila import exception
from manila.i18n import _
from . import constants

LOG = log.getLogger(__name__)


class DriverConfig(object):
    """
    1. 使用lxml模块解析manila xml文件
    2. 校验配置项的值是否正确
    3. 将配置值设定在类实例属性中，供其它模块直接调用
    """
    def __init__(self, config):
        self.config = config
        self.last_modify_time = None

    @staticmethod
    def check_config_exist(text, config_param):
        """
        校验配置参数是否设置或是否设置为空
        :param text: 配置参数的值
        :param config_param: 配置参数
        :return:
        """
        if not text or not text.strip():
            msg = _("Config file invalid. %s must be set.") % config_param
            LOG.error(msg)
            raise exception.BadConfigurationException(reason=msg)

    def get_xml_info(self):
        """
        使用lxml模块解析huawei manila配置文件
        :return: 返回解析后的xml对象
        """
        tree = ET.parse(self.config.manila_huawei_conf_file,
                        ET.XMLParser(resolve_entities=False))
        xml_root = tree.getroot()
        return xml_root

    def update_configs(self):
        """
        校验并更新所有config属性的值
        :return:
        """
        file_time = os.stat(self.config.manila_huawei_conf_file).st_mtime
        if self.last_modify_time == file_time:
            return

        self.last_modify_time = file_time
        xml_root = self.get_xml_info()

        attr_funcs = (
            self._nas_rest_url,
            self._nas_user,
            self._nas_password,
            self._nas_product,
            self._account_name,
            self._nas_storage_pools,
            self._reserved_percentage,
            self._max_over_ratio,
            self._ssl_verify,
            self._nas_domain,
            self._ssl_cert_path,
            self._semaphore,
            self._hot_disk_type,
            self._warm_disk_type,
            self._cold_disk_type,
            self._dpc_mount_options,
            self._nfs_mount_options
        )

        for f in attr_funcs:
            f(xml_root)

        return

    def _nas_rest_url(self, xml_root):
        text = xml_root.findtext('Storage/RestURL')
        self.check_config_exist(text, 'Storage/RestURL')
        setattr(self.config, 'rest_url', text.strip())

    def _nas_user(self, xml_root):
        text = xml_root.findtext('Storage/UserName')
        self.check_config_exist(text, 'Storage/UserName')
        setattr(self.config, 'user_name', text.strip())

    def _account_name(self, xml_root):
        text = xml_root.findtext('Filesystem/AccountName')
        if self.config.product == constants.PRODUCT_PACIFIC:
            self.check_config_exist(text, 'Filesystem/AccountName')
            setattr(self.config, 'account_name', text.strip())

    def _nas_password(self, xml_root):
        text = xml_root.findtext('Storage/UserPassword')
        self.check_config_exist(text, 'Storage/UserPassword')
        setattr(self.config, 'user_password', text.strip())

    def _nas_product(self, xml_root):
        text = xml_root.findtext('Storage/Product')
        self.check_config_exist(text, 'Storage/Product')

        if text.strip() not in constants.VALID_PRODUCTS:
            msg = _("Invalid storage product %(text)s, must be "
                    "in %(valid)s."
                    ) % {'text': text,
                         'valid': constants.VALID_PRODUCTS}
            LOG.error(msg)
            raise exception.BadConfigurationException(reason=msg)

        setattr(self.config, 'product', text.strip())

    def _nas_storage_pools(self, xml_root):
        text = xml_root.findtext('Filesystem/StoragePool')
        self.check_config_exist(text, 'Storage/StoragePool')

        if self.config.product == constants.PRODUCT_PACIFIC:
            pool_is_digit_list = [pool_id.strip().isdigit() for pool_id in text.split(';')]
            if False in pool_is_digit_list:
                err_msg = _("Filesystem/StoragePool value must be int.")
                LOG.error(err_msg)
                raise exception.BadConfigurationException(reason=err_msg)
            pool_list = set(int(pool_id.strip()) for pool_id in text.split(';'))
            setattr(self.config, 'pool_list', list(pool_list))
        else:
            pool_list = set(pool.strip() for pool in text.split(';'))
            setattr(self.config, 'pool_list', list(pool_list))

    def _reserved_percentage(self, xml_root):
        text = xml_root.findtext('Storage/Reserved_percentage')

        if not text or not text.strip():
            setattr(self.config, 'reserved_percentage', 15)
        elif not text.strip().isdigit():
            err_msg = _("Storage/Reserved_percentage must be int.")
            LOG.error(err_msg)
            raise exception.BadConfigurationException(reason=err_msg)
        else:
            setattr(self.config, 'reserved_percentage', text.strip())

    def _max_over_ratio(self, xml_root):
        text = xml_root.findtext('Storage/Max_over_subscription_ratio')

        if not text or not text.strip():
            setattr(self.config, 'max_over_ratio', 1)
        elif not text.strip().isdigit():
            err_msg = _("Storage/Max_over_subscription_ratio must be int.")
            LOG.error(err_msg)
            raise exception.BadConfigurationException(reason=err_msg)
        else:
            setattr(self.config, 'max_over_ratio', text.strip())

    def _nas_domain(self, xml_root):
        text = xml_root.findtext('Filesystem/ClusterDomainName')
        setattr(self.config, 'domain', text)

    def _ssl_verify(self, xml_root):
        text = xml_root.findtext('Storage/SslCertVerify')
        if not text or not text.strip():
            setattr(self.config, 'ssl_verify', False)
        else:
            setattr(self.config, 'ssl_verify', text.strip())

    def _ssl_cert_path(self, xml_root):
        text = xml_root.findtext('Storage/SslCertPath')
        if not text or not text.strip():
            setattr(self.config, 'ssl_cert_path', '')
        else:
            setattr(self.config, 'ssl_cert_path', text.strip())

    def _semaphore(self, xml_root):
        text = xml_root.findtext('Storage/Semaphore')
        if not text or not text.strip():
            setattr(self.config, 'semaphore', constants.DEFAULT_SEMAPHORE)
        elif not text.strip().isdigit():
            err_msg = _("Storage/Semaphore must be int. Configured value is %s") % text
            LOG.error(err_msg)
            raise exception.BadConfigurationException(reason=err_msg)
        elif int(text.strip()) > constants.DEFAULT_SEMAPHORE:
            err_msg = _("Storage/Semaphore configuration:%s "
                        "can not exceed %s") % (text, constants.DEFAULT_SEMAPHORE)
            LOG.error(err_msg)
            raise exception.BadConfigurationException(reason=err_msg)
        else:
            setattr(self.config, 'semaphore', int(text.strip()))

    def _hot_disk_type(self, xml_root):
        text = xml_root.findtext('Storage/HotDiskType')
        if not text or not text.strip():
            setattr(self.config, 'hot_disk_type', '')
        elif text.strip() not in constants.SUPPORT_DISK_TYPES:
            err_msg = _("Storage/HotDiskType configuration:%s "
                        "must in %s") % (text.strip(), constants.SUPPORT_DISK_TYPES)
            LOG.error(err_msg)
            raise exception.BadConfigurationException(reason=err_msg)
        else:
            setattr(self.config, 'hot_disk_type', text.strip())

    def _warm_disk_type(self, xml_root):
        text = xml_root.findtext('Storage/WarmDiskType')
        if not text or not text.strip():
            setattr(self.config, 'warm_disk_type', '')
        elif text.strip() not in constants.SUPPORT_DISK_TYPES:
            err_msg = _("Storage/WarmDiskType configuration:%s "
                        "must in %s") % (text.strip(), constants.SUPPORT_DISK_TYPES)
            LOG.error(err_msg)
            raise exception.BadConfigurationException(reason=err_msg)
        else:
            setattr(self.config, 'warm_disk_type', text.strip())

    def _cold_disk_type(self, xml_root):
        text = xml_root.findtext('Storage/ColdDiskType')
        if not text or not text.strip():
            setattr(self.config, 'cold_disk_type', '')
        elif text.strip() not in constants.SUPPORT_DISK_TYPES:
            err_msg = _("Storage/ColdDiskType configuration:%s "
                        "must in %s") % (text.strip(), constants.SUPPORT_DISK_TYPES)
            LOG.error(err_msg)
            raise exception.BadConfigurationException(reason=err_msg)
        else:
            setattr(self.config, 'cold_disk_type', text.strip())

    def _dpc_mount_options(self, xml_root):
        text = xml_root.findtext('DPC/MountOption')
        if not text or not text.strip():
            setattr(self.config, 'dpc_mount_option', '')
        else:
            setattr(self.config, 'dpc_mount_option', text.strip())

    def _nfs_mount_options(self, xml_root):
        text = xml_root.findtext('NFS/MountOption')
        if not text or not text.strip():
            setattr(self.config, 'nfs_mount_option', '')
        else:
            setattr(self.config, 'nfs_mount_option', text.strip())
