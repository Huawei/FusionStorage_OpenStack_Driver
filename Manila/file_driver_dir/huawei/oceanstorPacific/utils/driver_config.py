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
import re

from lxml import etree as ET
from oslo_log import log

from manila import exception
from manila.i18n import _
from . import constants
from . import cipher

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

    @staticmethod
    def check_config_numeric(text):
        """check a string is a float or a digit"""
        if text.isdigit():
            return True
        try:
            float(text)
        except Exception as err:
            LOG.debug("%s is not a float", text)
            return False
        else:
            return True

    @staticmethod
    def _optimized_pools_type_structure(pools_type):
        """
        Set pools type from:
        {
            0: {resource_pool: 0, pool_type:[NFS_SSD], qos_coefficients: [0-120-25000]},
            1: {
                resource_pool: 1,
                pool_type:[NFS_HDD, DPC_SSD],
                qos_coefficients: [200-20-10000, 0-480-130000]
            }
         }
         to:
         {
            0: {pool_qos_param:{NFS_SSD: 0-120-25000}},
            1: {
                pool_qos_param:{NFS_HDD: 200-20-10000, DPC_SSD: 0-480-130000}
            }
         }
        :param pools_type: dict: pools_type before optimized structure
        :return: dict: pools_type after optimized structure
        """
        optimized_pools_type = {}
        for pool_id, pool_value in pools_type.items():
            pool_type_list = pool_value.get('pool_type')
            qos_coefficient_list = pool_value.get('qos_coefficients')
            pool_qos_param = {}
            for index, value in enumerate(pool_type_list):
                pool_qos_param[value] = qos_coefficient_list[index]
            optimized_pools_type[pool_id] = {'pool_qos_param': pool_qos_param}
        return optimized_pools_type

    @staticmethod
    def _check_pool_type(pool_type_list, pool_type_info):
        """
        Check pool_type configure is valid or not
        """
        for pool_type in pool_type_list:
            if pool_type not in constants.POOL_TYPE_LIST:
                msg = ("Configuration pool_type:'%s' must in %s" %
                       (pool_type, constants.POOL_TYPE_LIST))
                LOG.error(msg)
                raise exception.BadConfigurationException(msg)
        pool_type_info['pool_type'] = pool_type_list

    @staticmethod
    def _check_qos_coefficients(qos_coefficients, pool_type_list, pool_type_info):
        """
        Check qos_coefficients configure is valid or not
        """
        qos_coefficient_list = qos_coefficients.split('&')
        if len(qos_coefficient_list) != len(pool_type_list):
            msg = ("Configuration qos_coefficients:%s length must equal to pool_type:%s" %
                   (qos_coefficient_list, pool_type_list))
            LOG.error(msg)
            raise exception.BadConfigurationException(msg)
        for qos_coefficient in qos_coefficient_list:
            coefficient_list = qos_coefficient.split('-')
            if len(coefficient_list) != 3:
                msg = ("Every qos_coefficient of qos_coefficients:%s length must equal to 3" %
                       qos_coefficients)
                LOG.error(msg)
                raise exception.BadConfigurationException(msg)
            for coefficient in coefficient_list:
                if not coefficient.isdigit():
                    msg = ("Every coefficient of qos_coefficients:%s must be integer" %
                           qos_coefficients)
                    LOG.error(msg)
                    raise exception.BadConfigurationException(msg)
        pool_type_info['qos_coefficients'] = qos_coefficient_list

    @staticmethod
    def _parser_ssl_value(ssl_value):
        if ssl_value is None:
            return False
        if str(ssl_value).strip().lower() in ('true', 'false'):
            return str(ssl_value).strip().lower() == 'true'
        else:
            msg = _("SSLCertVerify configured error, Please set this parameter to true or false.")
            LOG.error(msg)
            raise exception.InvalidInput(reason=msg)

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
            self._nfs_mount_options,
            self._rollback_rate,
            self._third_platform,
            self._share_backend_pools_type,
            self._check_ssl_two_way_config_valid
        )

        for f in attr_funcs:
            f(xml_root)

        self.last_modify_time = file_time
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
        setattr(self.config, 'user_password', cipher.decrypt_cipher(text.strip()))

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
        self.check_config_exist(text, 'Filesystem/StoragePool')

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
            setattr(self.config, 'reserved_percentage', constants.DEFAULT_RESERVED_PERCENT)
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
        elif not self.check_config_numeric(text.strip()):
            err_msg = _("Storage/Max_over_subscription_ratio must be float.")
            LOG.error(err_msg)
            raise exception.BadConfigurationException(reason=err_msg)
        elif float(text.strip()) < 1.0:
            err_msg = _("Storage/Max_over_subscription_ratio can not lower than 1.0.")
            LOG.error(err_msg)
            raise exception.BadConfigurationException(reason=err_msg)
        else:
            setattr(self.config, 'max_over_ratio', text.strip())

    def _nas_domain(self, xml_root):
        text = xml_root.findtext('Filesystem/ClusterDomainName')
        setattr(self.config, 'domain', text)

    def _ssl_verify(self, xml_root):
        text = xml_root.findtext('Storage/SslCertVerify')
        text = self._parser_ssl_value(text)
        setattr(self.config, 'ssl_verify', text)

    def _ssl_cert_path(self, xml_root):
        text = xml_root.findtext('Storage/SslCertPath')
        if not text or not text.strip():
            setattr(self.config, 'ssl_cert_path', '')
        else:
            setattr(self.config, 'ssl_cert_path', text.strip())

    def _semaphore(self, xml_root):
        text = xml_root.findtext('Storage/Semaphore')
        default_semaphore = constants.DEFAULT_PACIFIC_SEMAPHORE
        max_semaphore = constants.DEFAULT_PACIFIC_SEMAPHORE
        if self.config == constants.PRODUCT_PACIFIC_GFS:
            default_semaphore = constants.DME_DEFAULT_SEMAPHORE
            max_semaphore = constants.DME_DEFAULT_SEMAPHORE

        if not text or not text.strip():
            setattr(self.config, 'semaphore', default_semaphore)
        elif not text.strip().isdigit():
            err_msg = _("Storage/Semaphore must be int. Configured value is %s") % text
            LOG.error(err_msg)
            raise exception.BadConfigurationException(reason=err_msg)
        elif int(text.strip()) > max_semaphore:
            err_msg = _("Storage/Semaphore configuration:%s "
                        "can not exceed %s") % (text, max_semaphore)
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

    def _rollback_rate(self, xml_root):
        text = xml_root.findtext('Filesystem/RollbackRate')
        if not text or not text.strip():
            setattr(self.config, 'rollback_rate', constants.SPEED_MEDIUM)
        elif not text.strip().isdigit():
            err_msg = _("Filesystem/RollbackRate must be int. Configured value is %s") % text
            LOG.error(err_msg)
            raise exception.BadConfigurationException(reason=err_msg)
        elif int(text.strip()) not in constants.SPEED_LEVEL_ENUM:
            err_msg = _("Filesystem/RollbackRate must be in %s. "
                        "Configured value is %s") % (constants.SPEED_LEVEL_ENUM, text)
            LOG.error(err_msg)
            raise exception.BadConfigurationException(reason=err_msg)
        else:
            setattr(self.config, 'rollback_rate', int(text.strip()))

    def _third_platform(self, xml_root):
        text = xml_root.findtext('Platform/ThirdPlatform')
        if not text or not text.strip():
            setattr(self.config, 'platform', constants.PLATFORM_ZTE)
        else:
            setattr(self.config, 'platform', text.strip())

    def _share_backend_pools_type(self, xml_root):
        """
        set pools_type attr for config object by share_backend_pools_type configuration
        :param xml_root:
        :return:
        """
        share_backend_pools_type = self.config.safe_get('pool_qos_params')
        pools_type = {}

        if share_backend_pools_type:
            self._parse_and_check_pools_type(share_backend_pools_type, pools_type)
            pools_type = self._optimized_pools_type_structure(pools_type)
        setattr(self.config, 'pools_type', pools_type)

    def _parse_and_check_pools_type(self, share_backend_pools_type, pools_type):
        """
        First parse every resource pool type info to a dict
        Second check is configuration if valid or not
        :param share_backend_pools_type: The configuration of all resource pools
        :param pools_type: dict: the final parse value
        """
        pools_type_list = share_backend_pools_type.split('\n')
        for pool_type_info in pools_type_list:
            if not pool_type_info:
                continue
            attr_list = re.split('[{;}]', pool_type_info)
            pool_type = {}
            for attr in attr_list:
                if not attr:
                    continue

                pair = attr.split(':', 1)
                pool_type[pair[0]] = pair[1]
            self._check_backend_pool_type(pool_type)
            pools_type[pool_type.get('resource_pool')] = pool_type

    def _check_backend_pool_type(self, pool_type_info):
        """
        Check one resource pool info is valid or not
        :param pool_type_info:the configuration value of a resource pool type after parsing.
        """
        resource_pool = pool_type_info.get('resource_pool', '').strip()
        pool_type = pool_type_info.get('pool_type', '').strip()
        qos_coefficients = pool_type_info.get('qos_coefficients', '').strip()
        # check positional param is configured or not
        if not all((resource_pool, pool_type, qos_coefficients)):
            msg = ("Configuration 'resource_pool','pool_type','qos_coefficients' must "
                   "be set in share_backend_pools_type in every resource pool,"
                   " pool type info is %s" % pool_type_info)
            LOG.error(msg)
            raise exception.BadConfigurationException(msg)

        self._check_resource_pool(resource_pool, pool_type_info)
        pool_type_list = pool_type.split('&')
        self._check_pool_type(pool_type_list, pool_type_info)
        self._check_qos_coefficients(qos_coefficients, pool_type_list, pool_type_info)

    def _check_resource_pool(self, resource_pool, pool_type_info):
        """
        Check resource_pool configure is valid or not
        """
        if self.config.product == constants.PRODUCT_PACIFIC:
            if not resource_pool.isdigit():
                err_msg = _("Configuration resource_pool:%s value must be int." % resource_pool)
                LOG.error(err_msg)
                raise exception.BadConfigurationException(reason=err_msg)
            resource_pool = int(resource_pool)
        pool_type_info['resource_pool'] = resource_pool
        if resource_pool not in self.config.pool_list:
            msg = ("Configuration resource_pool %s must in XML StoragePool Configuration %s" %
                   (resource_pool, self.config.pool_list))
            LOG.error(msg)
            raise exception.BadConfigurationException(msg)

    def _check_ssl_two_way_config_valid(self, xml_root):
        """
        Check whether the two-way authentication parameter is valid or configured.
        """
        if not self.config.safe_get('storage_ssl_two_way_auth'):
            setattr(self.config, 'mutual_authentication', {})
            return
        self.check_config_exist(self.config.safe_get('storage_cert_filepath'),
                                constants.CONF_STORAGE_CERT_FILEPATH)
        self.check_config_exist(self.config.safe_get('storage_ca_filepath'),
                                constants.CONF_STORAGE_CA_FILEPATH)
        self.check_config_exist(self.config.safe_get('storage_key_filepath'),
                                constants.CONF_STORAGE_KEY_FILEPATH)
        mutual_authentication = {
            "storage_ca_filepath": self.config.safe_get('storage_ca_filepath'),
            "storage_key_filepath": self.config.safe_get('storage_key_filepath'),
            "storage_cert_filepath": self.config.safe_get('storage_cert_filepath'),
            "storage_ssl_two_way_auth": self.config.safe_get('storage_ssl_two_way_auth')
        }
        if self.config.safe_get('storage_key_pwd'):
            mutual_authentication["storage_key_pwd"] = self.config.safe_get('storage_key_pwd')
        setattr(self.config, 'mutual_authentication', mutual_authentication)
