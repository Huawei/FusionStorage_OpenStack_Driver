# Copyright (c) 2013 - 2016 Huawei Technologies Co., Ltd.
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
"""
Driver for Huawei Dsware.
"""

import traceback
import json
import os
import re
import time

from oslo_config import cfg
from oslo_log import log as logging
try:
    from oslo_service import loopingcall
except Exception as import_e:
    from cinder.openstack.common import loopingcall
from oslo_utils import strutils

from cinder import exception
from cinder.image import image_utils
from cinder.i18n import _, _LE, _LI, _LW
from cinder import utils
from cinder.volume import driver
from cinder.volume import qos_specs
from cinder.volume import volume_types
from cinder.volume import utils as volume_utils
from oslo_utils import units

from cinder.volume.drivers import fspythonapi

from cinder import context as cinder_context
import socket
import uuid

from tooz import coordination

from cinder import context
import datetime

LOG = logging.getLogger(__name__)

volume_opts = [
    cfg.BoolOpt('dsware_isthin',
                default=False,
                help='default isthin flag value'),
    cfg.StrOpt('dsware_manager',
               default='',
               help='fusionstorage manager ip addr for the cinder-volume'),
    cfg.StrOpt('fusionstorageagent',
               default='',
               help='fusionstorage_agent ip addr range.'),
    cfg.StrOpt('pool_type',
               default='default',
               help='pool type, like sata-2copy'),
    cfg.ListOpt('pool_id_list',
                default=[],
                help='pool id permit to use'),
    cfg.IntOpt('clone_volume_timeout',
               default=6800000,
               help='create clone volume timeout'),
    cfg.IntOpt('quickstart_max_link_num',
               default=128,
               help='quickstart max_link_num for dsware'),
    cfg.IntOpt('quickstart_clone_snapshot_timeout',
               default=6800000,
               help='quickstart duplicate snapshot timeout'),
    cfg.IntOpt('quickstart_lock_timeout',
               default=3600000,
               help='quickstart lock timeout'),
    cfg.IntOpt('quickstart_retry_times',
               default=10,
               help='quick create volume retry times'),
    cfg.IntOpt('quickstart_create_master_num',
               default=1,
               help='the number of master snap that simultaneously create'),
    cfg.BoolOpt('quickstart_delete_master',
                default=False,
                help='flag whether to delete master snap when count is zero'),
    cfg.BoolOpt('quickstart_create_master_snap_in_all_pool',
                default=True,
                help='flag whether to create master snap in all pool'),
    cfg.ListOpt('over_ratio',
                default=[1.0],
                help='Ratio of thin provisioning'),
    cfg.BoolOpt('IS_FC',
                default=False,
                help='compatible with FC Driver Volume'),
    cfg.IntOpt('quickstart_interval_one_timeout',
               default=10,
               help='first query quickstart volume timeout'),
    cfg.IntOpt('quickstart_interval_five_timeout',
               default=720,
               help='second query quickstart volume timeout'),
    cfg.DictOpt('manager_ips',
                default={},
                help='This option is to support the FSA to mount across the '
                     'different nodes. The parameters takes the standard dict '
                     'config form, manager_ips = host1:ip1, host2:ip2...'),
    cfg.IntOpt('scan_device_timeout',
               default=10,
               help='wait timeout for scanning target device finish'),
    cfg.BoolOpt('cross_node_detach',
                default=True,
                help='support cross vbs node detach for storage'),
]

CONF = cfg.CONF
CONF.register_opts(volume_opts)
volume_opts.append(
    cfg.StrOpt('storage_pool_aliases',
               default='',
               help='storage_pool_aliases'))
OLD_VERSION = 1
NEW_VERSION = 0
POOL_ID_LEN = 2
QUERY_TIMES_OF_CLONE_VOLUME = 10

HUAWEI_VALID_KEYS = ['maxIOPS', 'minIOPS', 'minBandWidth',
                     'maxBandWidth', 'maxMBPS', 'latency', 'IOType', 'IOPriority',
                     'IOPSLIMIT', 'MAXIOPSLIMIT', 'MINIOPSLIMIT',
                     'MBPSLIMIT', 'MAXMBPSLIMIT', 'MINMBPSLIMIT', 'total_iops_sec', 'total_bytes_sec']
LOWER_LIMIT_KEYS = ['MINIOPS', 'LATENCY', 'MINBANDWIDTH', 'MINMBPS']
UPPER_LIMIT_KEYS = ['MAXIOPS', 'MAXBANDWIDTH', 'MAXMBPS', 'TOTAL_IOPS_SEC', 'TOTAL_BYTES_SEC']

FSP_QOS_INFO = {
    'IOPSLIMIT': '',
    'MAXIOPSLIMIT': '',
    'MINIOPSLIMIT': '',
    'MBPSLIMIT': '',
    'MAXMBPSLIMIT': '',
    'MINMBPSLIMIT': ''
}


class DSWAREDriver(driver.VolumeDriver):
    """Huawei FusionStorage Driver."""
    VERSION = '1.0'

    # ThirdPartySystems wiki page
    CI_WIKI_NAME = "Huawei_FusionStorage_CI"

    DSWARE_VOLUME_CREATE_SUCCESS_STATUS = 0
    DSWARE_VOLUME_DUPLICATE_VOLUME = 6
    DSWARE_VOLUME_CREATING_STATUS = 7

    DSWARE_SNAP_CREATE_SUCCESS_STATUS = 0
    DSWARE_SNAP_CREATING_STATUS = 5

    def __init__(self, *args, **kwargs):
        super(DSWAREDriver, self).__init__(*args, **kwargs)
        self.configuration.append_config_values(volume_opts)
        manager_ip = self.configuration.dsware_manager
        agent_ip = self.configuration.fusionstorageagent
        if not manager_ip and not agent_ip:
            LOG.info("get manager ip and agent ip from [DEFAULT] cfg")
            manager_ip = CONF.dsware_manager
            agent_ip = CONF.fusionstorageagent
        LOG.info("DSWAREDriver manage %s, agent %s" % (manager_ip, agent_ip))
        self.dsware_client = fspythonapi.FSPythonApi(manager_ip, agent_ip)
        self.check_cloned_interval = 2
        self.check_quickstart_interval_one = 1
        self.check_quickstart_interval_five = 5
        self.pool_id_list = self.configuration.pool_id_list

        self.over_ratio = []
        over_ratio = self.configuration.get('over_ratio')
        if over_ratio is None:
            self.over_ratio = [1.0]
        else:
            LOG.info(_LI("[DSW-DRIVER] over_ratio [%s]"), over_ratio)
            self.over_ratio = over_ratio

        if len(self.pool_id_list) > len(self.over_ratio):
            if len(self.over_ratio) == 1:
                append_value = float(self.over_ratio[0])
            else:
                append_value = 1.0
            for i in range(len(self.pool_id_list) - len(self.over_ratio)):
                self.over_ratio.append(append_value)

        for index in range(len(self.over_ratio)):
            self.over_ratio[index] = float(self.over_ratio[index])

        self.pool_ratio_dict = dict(zip(self.pool_id_list, self.over_ratio))
        LOG.info(_LI("[DSW-DRIVER] pool_ratio [%s]"), self.pool_ratio_dict)

    def check_for_setup_error(self):
        # old version
        if self.dsware_version == OLD_VERSION:
            pool_id = 0
            pool_info = self.dsware_client.query_pool_info(pool_id)
            result = pool_info['result']
            if result != 0:
                msg = _("DSWARE query pool failed! Result:%s") % result
                raise exception.VolumeBackendAPIException(data=msg)
        # new version
        elif self.dsware_version == NEW_VERSION:
            pool_sets = self.dsware_client.query_pool_id_list(
                self.pool_id_list)
            if not pool_sets:
                msg = _("DSWARE query pool failed!")
                raise exception.VolumeBackendAPIException(data=msg)
        else:
            LOG.error(_LE("query dsware version failed!"))
            msg = _("DSWARE query dsware version failed!")
            raise exception.VolumeBackendAPIException(data=msg)

    def do_setup(self, context):
        # get pool_id_list
        self.pool_id_list = self.configuration.pool_id_list

        self.dsware_client.start_api_server()
        # get dsware version
        self.dsware_version = self.dsware_client.query_dsware_version()
        # add for compatible with old single_pool configuration
        if not self.pool_id_list:
            self.dsware_version = 1

        # In case cinder-volume restarts when quick_start_volume is creating,
        # but it will reschedule to another cinder-volume node and the volume
        # is created successfully at last. There may exist downloading status
        # template_snap or downloading status master_snap remain in the first
        # cinder-volume node,but clear_download func will not clear these snap,
        # for template_snap or master_snap is not in the volume db table,
        # so we need to clear in the do_setup func.
        self._clear_linkclone_download(context)
        LOG.info(_LI("DSWARE Driver do_setup finish."))

    def _clear_linkclone_download(self, context):
        host_id = socket.gethostname()
        try:
            dsw_manager_ip = self.dsware_client.get_manage_ip()
            snap_remained = self.db.link_clone_templates_get_all_by_host(
                context,
                {'host': host_id, 'status': 'downloading'})
            if snap_remained is not None:
                LOG.info(_LI("[DSW-DRIVER] [%(remain_snap_num)s] downloading"
                             " snapshots remained on host [%(host_id)s],"
                             "delete these following")
                         % {'remain_snap_num': len(list(snap_remained)),
                            'host_id': host_id})

                for snap_tmp in snap_remained:
                    self.db.link_clone_templates_destroy(context,
                                                         snap_tmp['id'])

                    if str(snap_tmp['is_template']).lower() == 'true':
                        self._delete_template_vol_remained(snap_tmp,
                                                           dsw_manager_ip)
                    self._delete_snapshot(snap_tmp['snap_name'])
        except Exception as e:
            LOG.error(_LE("[DSW-DRIVER] clear remained downloading "
                          "snapshot failed %s" % e))

    @staticmethod
    def _get_dsware_manage_ip(volume):
        volume_metadata = volume.get('volume_metadata')
        if volume_metadata:
            for metadata in volume_metadata:
                if metadata.key.lower() == 'manager_ip':
                    return metadata.value.lower()

            msg = _("DSWARE get manager ip failed!")
            raise exception.VolumeBackendAPIException(data=msg)
        else:
            msg = _("DSWARE get manager ip failed,volume metadata is null!")
            raise exception.VolumeBackendAPIException(data=msg)

    @staticmethod
    def _get_poolid_from_host(host):
        # multi_pool host format: 'hostid@backend#poolid'
        # single_pool host format: 'hostid@backend#backend'
        # other formats: the pool id would be zero.
        if host:
            if len(host.split('#', 1)) == POOL_ID_LEN:
                if host.split('#')[1].isdigit():
                    return int(host.split('#')[1])
        return 0

    def _get_dsware_volume_name(self, volume):
        provider_location = volume.get('provider_location', None)
        # provider_location maybe an empty string
        if provider_location:
            volume_provider_location = json.loads(provider_location)
            vol_name = volume_provider_location.get('vol_name', None)
            if vol_name is not None:
                return vol_name

        # When create quick_start image_volume in POD A, then we attach
        # the quick_start image_volume to a VM in another POD B. So the
        # manage method of Cinder dsware driver in POD B will be called.
        # The image_volume maybe still in creating template snap
        # or master snap stage. And the provider_location of image_volume
        # is still None.Then
        vol_name = self._construct_dsware_volume_name(volume)
        return vol_name

    @staticmethod
    def _construct_dsware_volume_name(volume):
        if CONF.IS_FC:
            return volume['id'].replace('-', '')
        else:
            return volume['name']

    def _get_dsware_snap_name(self, snapshot):
        provider_location = snapshot.get('provider_location', None)
        if provider_location:
            snap_provider_location = json.loads(provider_location)
            snap_name = snap_provider_location.get('snap_name', None)
            if snap_name is not None:
                return snap_name

        # It should not enter this case
        snap_name = self._construct_dsware_snap_name(snapshot)
        return snap_name

    @staticmethod
    def _construct_dsware_snap_name(snapshot):
        if CONF.IS_FC:
            return snapshot['id'].replace('-', '')
        else:
            return snapshot['name']

    def _create_volume(self, volume_id, volume_size, is_thin, volume_host,
                       is_encrypted=None, volume_cmk_id=None,
                       volume_auth_token=None):
        pool_id = self._get_poolid_from_host(volume_host)

        try:
            result = self.dsware_client.create_volume(
                volume_id, pool_id, volume_size, int(is_thin), is_encrypted,
                volume_cmk_id, volume_auth_token)
        except Exception as e:
            LOG.error(_LE("create volume error, details: %s"), e)
            raise e

        if result != 0:
            msg = _("DSWARE Create Volume failed! Result:%s") % result
            raise exception.VolumeBackendAPIException(data=msg)
        else:
            create_volume_info = self.dsware_client.query_volume(volume_id)
            if create_volume_info['result'] != 0:
                msg = _("DSWARE Query Volume failed! Result:%s") % result
                raise exception.VolumeBackendAPIException(data=msg)

        return create_volume_info

    def _check_qos_specs(self, qos_specs_id):
        """
        check if QoS related according to qos info
        :param qos_specs_id:
        :return: True/False/None
        """
        if not qos_specs_id:
            LOG.info(_LI("DSWARE get null qos_specs_id"))
            return None

        qos_spec = self._get_qos_specs(qos_specs_id)

        if not qos_spec or qos_spec['result'] == "not support":
            LOG.warning(_LW("DSWARE can't get qos info,\
             can't get request type"))
            return None

        if qos_spec.get('IOPSLIMIT') != '' \
                or qos_spec.get('MAXIOPSLIMIT') != '' \
                or qos_spec.get('MINIOPSLIMIT') != '' \
                or qos_spec.get('MBPSLIMIT') != '' \
                or qos_spec.get('MAXMBPSLIMIT') != '' \
                or qos_spec.get('MINMBPSLIMIT') != '' \
                or qos_spec.get('TOTAL_IOPS_SEC') != '' \
                or qos_spec.get('TOTAL_BYTES_SEC') != '' \
                or qos_spec.get('MAXIOPS') != '' \
                or qos_spec.get('MAXBANDWIDTH') != '' \
                or qos_spec.get('MAXMBPS') != '':
            return True
        else:
            return False

    def _check_qos_type(self, volume=None, volume_type=None):
        """
        check if it is FusionCloud 6.5 private cloud for QoS related
        :param volume:
        :param volume_type:
        :return: Ture -- is
                  False -- not
                  None -- don't know
        """
        LOG.info(_LI("DSWARE _check_qos_type"))
        if not volume and volume_type:
            # check according to volume_type
            qos_specs_id = self._get_type_qos_id(volume_type)
        elif volume and not volume_type:
            # check according to volume
            qos_specs_id = self._get_volume_qos_id(volume)
        else:
            # error
            LOG.warning(_LW("DSWARE null volume and null volume_type,\
             can't get request volume_type"))
            return None

        return self._check_qos_specs(qos_specs_id)

    def create_volume(self, volume):
        # Creates a volume in dsware
        LOG.debug(_LI("begin to create volume %s in dsware"), volume['name'])
        dsware_volume_name = self._construct_dsware_volume_name(volume)
        volume_size = volume['size']
        volume_host = volume['host']
        is_thin = self.configuration.dsware_isthin
        volume_metadata = volume.get('volume_metadata')
        if volume_metadata is not None:
            for metadata in volume_metadata:
                if metadata.key.lower() == 'isthin' and (
                        metadata.value.lower() == 'true'):
                    is_thin = True
                else:
                    is_thin = False
        # change GB to MB
        volume_size *= units.Ki
        is_encrypted = None
        volume_cmk_id = None
        volume_auth_token = None
        volume_meta_data = volume.get('metadata')
        if volume_meta_data:
            if '__system__encrypted' in volume_meta_data:
                is_encrypted = volume_meta_data.get('__system__encrypted',
                                                    None)
            if '__system__cmkid' in volume_meta_data:
                volume_cmk_id = volume_meta_data.get('__system__cmkid', None)
                volume_auth_token = volume._context.auth_token
                self._check_encrypted_metadata_valid(is_encrypted, volume_cmk_id)

        dsw_manager_ip = self.dsware_client.get_manage_ip()
        meta_data = {}
        if volume_metadata is not None:
            for metadata in volume_metadata:
                meta_data.update({metadata.key: metadata.value})

        provider_location = {}
        pool_id = self._get_poolid_from_host(volume_host)
        if CONF.IS_FC:
            # FC Driver Volume
            meta_data['StorageType'] = 'FC_DSWARE'
            meta_data['volInfoUrl'] = 'fusionstorage://' + str(
                dsw_manager_ip) + '/' + str(
                pool_id) + '/' + dsware_volume_name
            hw_passthrough = meta_data.get('hw:passthrough', None)
            if hw_passthrough and str(hw_passthrough).lower() == 'true':
                provider_location['offset'] = 0
            else:
                provider_location['offset'] = 4
        else:
            meta_data.update({"manager_ip": dsw_manager_ip,
                              'StorageType': 'FusionStorage'})
            provider_location['offset'] = 0
        provider_location['storage_type'] = meta_data['StorageType']
        provider_location['ip'] = dsw_manager_ip
        provider_location['pool'] = pool_id
        provider_location['vol_name'] = dsware_volume_name

        if provider_location['offset'] == 4:
            volume_size += 1
        create_volume_info = self._create_volume(dsware_volume_name, volume_size, is_thin,
                                                 volume_host, is_encrypted, volume_cmk_id,
                                                 volume_auth_token)

        replication_driver_data = {'ip': dsw_manager_ip,
                                   'pool': pool_id,
                                   'vol_name': dsware_volume_name}
        meta_data['lun_wwn'] = create_volume_info.get('wwn')
        volume_info = {
            "metadata": meta_data,
            "provider_location": json.dumps(provider_location),
            "replication_driver_data": json.dumps(replication_driver_data)
        }

        # create volume qos.
        self._create_and_associate_qos_for_volume(volume)
        return volume_info

    def _create_volume_from_snap(self, volume_id, volume_size, snapshot_name):
        result = self.dsware_client.create_volume_from_snap(
            volume_id, volume_size, snapshot_name)
        if result != 0:
            msg = _("DSWARE:create volume from snap fails,result:%s") % result
            raise exception.VolumeBackendAPIException(data=msg)
        else:
            create_volume_info = self.dsware_client.query_volume(volume_id)
            result = create_volume_info['result']
            if create_volume_info['result'] != 0:
                msg = _("DSWARE Query Volume failed! Result:%s") % result
                raise exception.VolumeBackendAPIException(data=msg)

        return create_volume_info

    def _create_fullvol_from_snap(self, volume_id, snapshot_name):
        result = self.dsware_client.create_fullvol_from_snap(volume_id,
                                                             snapshot_name)
        if result != 0:
            msg = (_("DSWARE:create full volume from snap fails,result:%s")
                   % result)
            raise exception.VolumeBackendAPIException(data=msg)

    def create_volume_from_snapshot(self, volume, snapshot):
        # Creates a volume from snapshot.
        dsware_volume_name = self._construct_dsware_volume_name(volume)
        volume_size = volume['size']
        snapshot_name = self._get_dsware_snap_name(snapshot)
        volume_metadata = volume['volume_metadata']
        if volume_size < int(snapshot['volume_size']):
            msg = _("DSWARE:volume size can not be less than snapshot size")
            raise exception.VolumeBackendAPIException(data=msg)
        # Change GB to MB.
        volume_size *= units.Ki

        dsw_manager_ip = self.dsware_client.get_manage_ip()
        meta_data = {}
        if volume_metadata is not None:
            for metadata in volume_metadata:
                meta_data.update({metadata.key: metadata.value})

        admin_context = cinder_context.get_admin_context()
        provider_location = {}
        pool_id = self._get_poolid_from_host(volume['host'])
        if CONF.IS_FC:
            # FC Driver Volume
            meta_data['StorageType'] = 'FC_DSWARE'
            meta_data['volInfoUrl'] = 'fusionstorage://' + str(
                dsw_manager_ip) + '/' + str(
                pool_id) + '/' + dsware_volume_name
        else:
            meta_data.update({"manager_ip": dsw_manager_ip,
                              'StorageType': 'FusionStorage'})

        provider_location['storage_type'] = meta_data['StorageType']
        provider_location['ip'] = dsw_manager_ip
        provider_location['pool'] = pool_id
        provider_location['vol_name'] = dsware_volume_name

        # inherit provider_location['offset']  from snapshot
        if snapshot.get('provider_location'):
            snap_provider_location = json.loads(snapshot['provider_location'])
            provider_location['offset'] = snap_provider_location['offset']
        else:
            # In pure kvm scene,after upgrade,before provider_location of
            # snap is filled, we can't get the value of provider_location.
            provider_location['offset'] = 0

        src_volume = self.db.volume_get(admin_context, snapshot['volume_id'])
        src_volume_metadata = self.db.volume_metadata_get(admin_context,
                                                          src_volume['id'])
        if meta_data.get('hw:passthrough') is not None:
            self._check_volume_hw_passthrough_metadata(src_volume_metadata,
                                                       meta_data)
        else:
            # inherit metadata['hw:passthrough'] from src_volume of snapshot
            if src_volume_metadata.get('hw:passthrough') is not None:
                meta_data['hw:passthrough'] = src_volume_metadata.get(
                    'hw:passthrough')

        # inherit encrypt metadata from src_volume of snapshot
        is_encrypted = src_volume_metadata.get('__system__encrypted', None)
        volume_cmk_id = src_volume_metadata.get('__system__cmkid', None)

        if meta_data.get('__system__encrypted') is not None:
            self._check_volume_encrypt_metadata(src_volume_metadata, meta_data)
        else:
            if is_encrypted is not None and volume_cmk_id is not None:
                meta_data['__system__encrypted'] = is_encrypted
                meta_data['__system__cmkid'] = volume_cmk_id
        self._check_encrypted_metadata_valid(
            meta_data.get('__system__encrypted'),
            meta_data.get('__system__cmkid'))

        if provider_location['offset'] == 4:
            volume_size += 1

        full_clone = meta_data.get('full_clone')
        if full_clone is None:
            full_clone = '0'
        elif str(full_clone) == '0':
            full_clone = '0'
        else:
            full_clone = '1'

        if full_clone == '0':
            self._create_volume_from_snap(dsware_volume_name, volume_size,
                                          snapshot_name)
        else:
            is_thin = self.configuration.dsware_isthin
            self._create_volume(dsware_volume_name, volume_size,
                                is_thin, volume['host'],
                                meta_data.get('__system__encrypted'),
                                meta_data.get('__system__cmkid'),
                                volume._context.auth_token)
            self._create_fullvol_from_snap(dsware_volume_name,
                                           snapshot_name)
            ret = self._wait_for_create_cloned_volume_finish_timer(
                dsware_volume_name)
            if not ret:
                msg = (_('Create full volume %s from snap failed')
                       % dsware_volume_name)
                raise exception.VolumeBackendAPIException(data=msg)

        replication_driver_data = {'ip': dsw_manager_ip,
                                   'pool': pool_id,
                                   'vol_name': dsware_volume_name}
        volume_info = {
            "metadata": meta_data,
            "provider_location": json.dumps(provider_location),
            "replication_driver_data": json.dumps(replication_driver_data)
        }

        # create voume qos.
        self._create_and_associate_qos_for_volume(volume)

        return volume_info

    @staticmethod
    def _check_encrypted_metadata_valid(is_encrypted, volume_cmk_id):
        if is_encrypted is not None and str(is_encrypted) == '1':
            if volume_cmk_id is None:
                raise exception.InvalidParameterValue(
                    err=_('__system__encrypted is set 1, '
                          'but __system__cmkid is not set'))

    @staticmethod
    def _check_volume_encrypt_metadata(src_meta, dst_meta):
        if src_meta is None or not isinstance(src_meta, dict):
            return
        if dst_meta.get('__system__encrypted') is None:
            return

        if str(dst_meta.get('__system__encrypted')) == '0':
            if not (src_meta.get('__system__encrypted') is None or
                    str(src_meta.get('__system__encrypted')) == '0'):
                raise exception.InvalidParameterValue(err=_(
                    '__system__encrypted conflict'))
        elif str(dst_meta.get('__system__encrypted')) == '1':
            if str(dst_meta.get('__system__encrypted')) != str(src_meta.get('__system__encrypted')) \
                    or str(dst_meta.get('__system__cmkid')) != str(src_meta.get('__system__cmkid')):
                raise exception.InvalidParameterValue(
                    err=_('__system__encrypted or __system__cmkid conflict'))
        else:
            raise exception.InvalidParameterValue(
                err=_('__system__encrypted is not 0 or 1'))

    @staticmethod
    def _check_volume_hw_passthrough_metadata(src_meta, dst_meta):
        if dst_meta.get('hw:passthrough') is None:
            return
        src_hw_passthrough = str(src_meta.get('hw:passthrough')).lower() \
            if src_meta.get('hw:passthrough') is not None else 'false'
        dst_hw_passthrough = str(dst_meta.get('hw:passthrough')).lower()
        if dst_hw_passthrough != src_hw_passthrough:
            raise exception.InvalidParameterValue(
                err=_('hw:passthrough conflict'))

    def create_cloned_volume(self, volume, src_volume):
        """
        dispatcher to dsware client create_volume_from_volume.
        wait volume create finished
        """
        dsware_volume_name = self._construct_dsware_volume_name(volume)
        volume_size = volume['size']
        src_volume_name = self._get_dsware_volume_name(src_volume)
        src_volume_size = src_volume['size']
        volume_metadata = volume.get('volume_metadata')

        if volume_size < src_volume_size:
            msg = (_('Cannot clone volume %(volume_name)s of size '
                     '%(volume_size)s from src volume %(src_volume_name)s '
                     'of size %(src_volume_size)s') %
                   {'volume_name': dsware_volume_name,
                    'volume_size': volume_size,
                    'src_volume_name': src_volume_name,
                    'src_volume_size': src_volume_size})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        pool_id = self._get_poolid_from_host(volume['host'])
        volume_size *= units.Ki
        is_encrypted = None
        volume_cmk_id = None
        volume_meta_data = src_volume.get('metadata')
        if volume_meta_data:
            if '__system__encrypted' in volume_meta_data:
                is_encrypted = volume_meta_data.get(
                    '__system__encrypted', None)
            if '__system__cmkid' in volume_meta_data:
                volume_cmk_id = volume_meta_data.get('__system__cmkid',
                                                     None)
        dsw_manager_ip = self.dsware_client.get_manage_ip()
        meta_data = {}
        if volume_metadata is not None:
            for metadata in volume_metadata:
                meta_data.update({metadata.key: metadata.value})
        if meta_data.get('__system__encrypted') is not None:
            self._check_volume_encrypt_metadata(volume_meta_data,
                                                meta_data)
        else:
            if is_encrypted is not None and volume_cmk_id is not None:
                meta_data['__system__encrypted'] = is_encrypted
                meta_data['__system__cmkid'] = volume_cmk_id
        self._check_encrypted_metadata_valid(
            meta_data.get('__system__encrypted'),
            meta_data.get('__system__cmkid'))

        provider_location = {}
        if CONF.IS_FC:
            # FC Driver Volume
            meta_data['StorageType'] = 'FC_DSWARE'
            meta_data['volInfoUrl'] = 'fusionstorage://' + str(
                dsw_manager_ip) + '/' + str(
                pool_id) + '/' + dsware_volume_name
        else:
            meta_data.update({"manager_ip": dsw_manager_ip,
                              'StorageType': 'FusionStorage'})

        provider_location['storage_type'] = meta_data['StorageType']
        provider_location['ip'] = dsw_manager_ip
        provider_location['pool'] = pool_id
        provider_location['vol_name'] = dsware_volume_name

        # inherit provider_location['offset'] from src_volume
        if src_volume.get('provider_location'):
            src_volume_provider_location = json.loads(
                src_volume['provider_location'])
            provider_location['offset'] = \
                src_volume_provider_location['offset']
        else:
            # In pure kvm scene,after upgrade,before provider_location of
            # volume is filled, we can't get the value of provider_location.
            provider_location['offset'] = 0

        if provider_location['offset'] == 4:
            volume_size += 1
        result = self.dsware_client.create_linked_clone_volume(dsware_volume_name,
                                                               volume_size,
                                                               src_volume_name)
        if result:
            msg = _('Clone volume %s failed') % dsware_volume_name
            raise exception.VolumeBackendAPIException(data=msg)

        create_volume_info = self.dsware_client.query_volume(dsware_volume_name)
        result = create_volume_info['result']
        if create_volume_info['result'] != 0:
            msg = _("DSWARE Query Volume failed! Result:%s") % result
            raise exception.VolumeBackendAPIException(data=msg)

        LOG.info('create linked clone volume success, '
                 '%(volume_name)s %(volume_size)s %(src_volume_name)s.',
                 {"volume_name": dsware_volume_name,
                  "volume_size": volume_size,
                  "src_volume_name": src_volume_name})

        replication_driver_data = {'ip': dsw_manager_ip,
                                   'pool': pool_id,
                                   'vol_name': dsware_volume_name}

        volume_info = {
            "metadata": meta_data,
            "provider_location": json.dumps(provider_location),
            "replication_driver_data": json.dumps(replication_driver_data)
        }

        # create volume qos.
        self._create_and_associate_qos_for_volume(volume)

        return volume_info

    def _check_create_cloned_volume_finish(self, new_volume_name):
        LOG.debug('Loopcall: _check_create_cloned_volume_finish(), '
                  'volume-name: %s.', new_volume_name)
        current_volume = self.dsware_client.query_volume(new_volume_name)

        if current_volume:
            status = current_volume['status']
            LOG.debug('Wait clone volume %(volume_name)s, status: % (status)s.',
                      {"volume_name": new_volume_name,
                       "status": status})
            if int(status) == self.DSWARE_VOLUME_CREATING_STATUS or int(
                    status) == self.DSWARE_VOLUME_DUPLICATE_VOLUME:
                self.count += 1
            elif int(status) == self.DSWARE_VOLUME_CREATE_SUCCESS_STATUS:
                tmp_snap_name = str(new_volume_name) + '_tmp_snap'
                self._delete_snapshot(tmp_snap_name)
                raise loopingcall.LoopingCallDone(retvalue=True)
            else:
                msg = _('Clone volume %(new_volume_name)s failed, '
                        'volume status is: % (status)s.')
                LOG.error(msg, {'new_volume_name': new_volume_name,
                                'status': status})
                raise loopingcall.LoopingCallDone(retvalue=False)
            if self.count > self.configuration.clone_volume_timeout:
                msg = _('Dsware clone volume time out. '
                        'Volume: %(new_volume_name)s, status: %(status)s')
                LOG.error(msg, {'new_volume_name': new_volume_name,
                                'status': current_volume['status']})
                raise loopingcall.LoopingCallDone(retvalue=False)
        else:
            LOG.warning(_LW('Can not find volume %s from dsware.'),
                        new_volume_name)
            self.count += 1
            if self.count > QUERY_TIMES_OF_CLONE_VOLUME:
                msg = _("Dsware clone volume failed: volume "
                        "can not be found from Dsware.")
                LOG.error(msg)
                raise loopingcall.LoopingCallDone(retvalue=False)

    def _wait_for_create_cloned_volume_finish_timer(self, new_volume_name):
        timer = loopingcall.FixedIntervalLoopingCall(
            self._check_create_cloned_volume_finish, new_volume_name)
        LOG.debug('Calling _check_create_cloned_volume_finish: volume-name %s',
                  new_volume_name)
        self.count = 0
        ret = timer.start(interval=self.check_cloned_interval).wait()
        timer.stop()
        return ret

    @staticmethod
    def _analyse_output(out):
        if out is not None:
            analyse_result = {}
            out_temp = out.split('\n')
            for line in out_temp:
                if re.search('^ret_code=', line):
                    analyse_result['ret_code'] = line[9:]
                elif re.search('^ret_desc=', line):
                    analyse_result['ret_desc'] = line[9:]
                elif re.search('^dev_addr=', line):
                    analyse_result['dev_addr'] = line[9:]
            return analyse_result
        else:
            return None

    @staticmethod
    def _dmsetup_create(source_dev, volume_name, volume_size):
        # shift 4K offset after attach FC volume to the host,
        # and create the dm device, while the name is:
        # 5F53812FBF0644DD9EA7422360193789-dm
        dm_device_name = "%s-dm" % volume_name
        dm_device = "/dev/mapper/%s" % dm_device_name
        # Before shift, dmsetup remove if exists dm device
        if os.path.lexists(dm_device):
            cmd_dmsetup_remove = ['dmsetup', 'remove', dm_device_name]
            out, err = utils.execute(*cmd_dmsetup_remove, run_as_root=True)
            LOG.info(_LI("dmsetup remove out is %s"), out)
        volume_size_sector = int(volume_size) * 1024 * 1024 * 1024 / 512
        dm_table = "0 %s linear %s 8" % (volume_size_sector, source_dev)
        cmd_dmsetup_create = ['dmsetup', 'create', dm_device_name,
                              '--table', dm_table]
        out, err = utils.execute(*cmd_dmsetup_create, run_as_root=True)
        LOG.info(_LI("dmsetup create cmd:%(args)s, out:%(result)s") %
                 {'args': cmd_dmsetup_create, 'result': out})
        return dm_device

    @staticmethod
    def _dmsetup_remove(volume_name):
        dm_device_name = "%s-dm" % volume_name
        dm_device = "/dev/mapper/%s" % dm_device_name
        # Before shift, dmsetup remove if exists dm device
        if os.path.lexists(dm_device):
            cmd_dmsetup_remove = ['dmsetup', 'remove', dm_device_name]
            out = None
            # fix bug, when copy image data to vol using cache,
            # the cache data may not be flushed into device, retry here to
            # improve the reliability
            for index in range(6):
                try:
                    out, err = utils.execute(*cmd_dmsetup_remove,
                                             run_as_root=True)
                except Exception as e:
                    LOG.info(_LI("dmsetup remove failed, some cache data may"
                                 " have not written to device %(device)s,"
                                 "count:%(index)s") %
                             {'device': dm_device, 'index': index})
                    if index == 5:
                        raise e
                    # retry time interval:5 5 5 30 30
                    sleep_time = 5 + (index / 3) * 25
                    time.sleep(sleep_time)
                else:
                    break
            LOG.info(_LI("dmsetup remove cmd:%(args)s, out:%(result)s") %
                     {'args': cmd_dmsetup_remove, 'result': out})

    def _query_volume_attach(self, volume_name, dsw_manager_ip):
        cmd = ['vbs_cli', '-c', 'querydevwithip', '-v', volume_name, '-i',
               dsw_manager_ip.replace('\n', ''), '-p', 0]
        out, err = self._execute(*cmd, run_as_root=True)
        analyse_result = self._analyse_output(out)
        LOG.info(_LI("vbs cmd is %s") % str(cmd))
        LOG.debug("_query_volume_attach out is %s", analyse_result)
        return analyse_result

    def _get_volume(self, volume_name):
        result = self.dsware_client.query_volume(volume_name)
        LOG.debug("result['result'] is %s", result['result'])
        if int(result['result']) == 50150005:
            LOG.debug("DSWARE get volume,volume is not exist.")
            return False
        elif int(result['result']) == 0:
            return True
        else:
            msg = _("DSWARE get volume failed!")
            raise exception.VolumeBackendAPIException(data=msg)

    def _delete_volume(self, volume_name):
        # step1 detach volume from host before delete volume
        # self._dsware_detach_volume(volume_name,dsw_manager_ip)
        # step2 delete volume
        result = self.dsware_client.delete_volume(volume_name)
        LOG.debug("DSWARE delete volume,result is %s", result)
        if int(result) == 50150005:
            LOG.debug("DSWARE delete volume,volume is not exist.")
            return False
        elif int(result) == 50151002:
            LOG.debug("DSWARE delete volume,volume is being deleted.")
            return False
        elif int(result) == 0:
            return True
        else:
            msg = _("DSWARE delete volume failed, result:%s.") % result
            raise exception.VolumeBackendAPIException(data=msg)

    def delete_volume(self, volume):
        # delete volume
        # step1 if volume is not exist,then return
        volume_name = self._get_dsware_volume_name(volume)
        LOG.debug("begin to delete volume in DSWARE: %s", volume_name)
        # if not self._get_volume(volume['name']):
        #    return True

        # delete qos
        vol_qos = self.query_volume_qos(volume)
        if vol_qos['result'] != 0:
            if int(vol_qos['result']) == 50150005:
                LOG.warning("DSWARE delete volume,volume is not exist.")
                return False
            elif int(vol_qos['result']) == 50151002:
                LOG.warning("DSWARE delete volume,volume being deleted.")
                return False
            else:
                msg = _(
                    "DSWARE Del vol %(volume)s, Qry Qos failed! Res:"
                    "%(result)s") % {'volume': volume_name,
                                     'result': vol_qos['result']}
                raise exception.VolumeBackendAPIException(data=msg)

        if self._check_openstack_qos_name(vol_qos):
            LOG.debug("found openstack qos: %s", str(vol_qos))
            result = self.dsware_client.disassociate_qos_with_volume(
                vol_qos['qos_name'], volume_name)
            if result != 0:
                if int(vol_qos['result']) == 50150005:
                    LOG.warning("DSWARE delete volume,volume not exist.")
                    return False
                elif int(vol_qos['result']) == 50151002:
                    LOG.warning("DSWARE del vol, vol is being deleted.")
                    return False
                else:
                    msg = _(
                        "disassociate QoS %(qos)s with vol %(volume)s"
                        " failed! Res:%(result)s.") % {
                              'qos': vol_qos['qos_name'], 'volume': volume_name,
                              'result': result}
                    raise exception.VolumeBackendAPIException(data=msg)

            result = self.dsware_client.delete_qos(vol_qos['qos_name'])
            if int(result) != 0 and int(result) != -157210:
                msg = _(
                    "DSWARE delete QoS %(qos)s with volume %(volume)s failed!"
                    "Result:%(result)s.") % {'qos': vol_qos['qos_name'],
                                             'volume': volume_name,
                                             'result': result}
                raise exception.VolumeBackendAPIException(data=msg)
        elif vol_qos['qos_name'] != '':
            # not cinder qos, disassociate qos only
            LOG.debug("not openstack qos: %s did not delete it", str(vol_qos))
            self._dsware_disasso_qos(vol_qos['qos_name'], volume_name)

        return self._delete_volume(volume_name)

    def _get_snapshot(self, snapshot_name):
        snapshot_info = self.dsware_client.query_snap(snapshot_name)
        LOG.debug("_get_snapshot snapshot_info is : %s", snapshot_info)
        if int(snapshot_info['result']) == 50150006:
            msg = _('Snapshot %s not found') % snapshot_name
            LOG.error(msg)
            return False
        elif int(snapshot_info['result']) == 0:
            return True
        else:
            msg = _("DSWARE get snapshot failed!")
            raise exception.VolumeBackendAPIException(data=msg)

    def _create_snapshot(self, snapshot_id, volume_id):
        LOG.debug("_create_snapshot %s to Dsware", snapshot_id)
        smart_flag = 0
        res = self.dsware_client.create_snapshot(snapshot_id,
                                                 volume_id,
                                                 smart_flag)
        if res != 0:
            msg = _("DSWARE Create Snapshot failed! res:%s") % res
            raise exception.VolumeBackendAPIException(data=msg)

    def _delete_snapshot(self, snapshot_id):
        LOG.debug("_delete_snapshot %s to Dsware", snapshot_id)
        res = self.dsware_client.delete_snapshot(snapshot_id)
        LOG.debug("_delete_snapshot res is : %s", res)
        if int(res) != 0 and int(res) != 50150006:
            raise exception.SnapshotIsBusy(snapshot_name=snapshot_id)

    def create_snapshot(self, snapshot):
        admin_context = cinder_context.get_admin_context()
        volume = self.db.volume_get(admin_context, snapshot['volume_id'])
        dsware_volume_name = self._get_dsware_volume_name(volume)
        dsware_snapshot_name = self._construct_dsware_snap_name(snapshot)

        enable_active = True
        snapshot_metadata = snapshot.get('metadata')
        if snapshot_metadata:
            if '__system__enableActive' in snapshot_metadata:
                enable_active = strutils.bool_from_string(
                    snapshot_metadata['__system__enableActive'],
                    default=True
                )
        if enable_active:
            self._create_snapshot(dsware_snapshot_name, dsware_volume_name)

        volume_metadata = self.db.volume_metadata_get(admin_context,
                                                      volume['id'])

        snap_provider_location = {}
        dsw_manager_ip = self.dsware_client.get_manage_ip()
        pool_id = self._get_poolid_from_host(volume['host'])
        if volume.get('provider_location'):
            volume_provider_location = json.loads(volume['provider_location'])
            snap_provider_location['offset'] = \
                volume_provider_location['offset']
            snap_provider_location['storage_type'] = \
                volume_provider_location['storage_type']
            snap_provider_location['ip'] = volume_provider_location['ip']
            snap_provider_location['pool'] = volume_provider_location['pool']
        else:
            # In pure kvm scene,after upgrade,before provider_location of
            # volume is filled, we can't get the value of provider_location.
            snap_provider_location['offset'] = 0
            snap_provider_location['storage_type'] = \
                volume_metadata.get('StorageType')
            snap_provider_location['ip'] = dsw_manager_ip
            snap_provider_location['pool'] = pool_id
        snap_provider_location['snap_name'] = dsware_snapshot_name

        provider_auth = {'ip': dsw_manager_ip,
                         'pool': pool_id,
                         'snap_name': dsware_snapshot_name,
                         'vol_name': dsware_volume_name}
        return {
            "provider_location": json.dumps(snap_provider_location),
            "provider_auth": json.dumps(provider_auth)
        }

    def active_snapshots(self, context, volume_id=None, volume_snapshots=None):
        """
        :param volume_id:
        :volume_type context: object
        """
        snapshot_list = []
        volume_list = []

        for snapshot in volume_snapshots:
            snapshot_location = json.loads(
                snapshot['snapshot_provider_location'])
            volume_name = snapshot_location['vol_name']
            snap_name = snapshot_location['snap_name']
            volume_list.append(volume_name)
            snapshot_list.append(snap_name)

        vol_list_str = ','.join(volume_list)
        snap_list_str = ','.join(snapshot_list)
        result = self.dsware_client.active_snapshots(vol_list_str,
                                                     snap_list_str)
        if result != 0:
            msg = _("DSWARE active snapshots failed! %s") % result
            raise exception.VolumeBackendAPIException(data=msg)

    def delete_snapshot(self, snapshot):
        snapshot_id = self._get_dsware_snap_name(snapshot)
        LOG.debug("delete_snapshot %s", snapshot_id)
        # if not self._get_snapshot(snapshot_id):
        #    return
        # else:
        self._delete_snapshot(snapshot_id)

    def _rollback_snapshot(self, snapshot_id, volume_id):
        result = self.dsware_client.rollback_snapshot(snapshot_id, volume_id)
        if int(result) != 0 and int(result) != 50153010:
            msg = _("DSWARE Rollback Snapshot failed! Result:%s") % result
            raise exception.VolumeBackendAPIException(data=msg)

    def rollback_snapshot(self, context, snapshot):
        # Rollback a volume to a snapshot point
        LOG.info(_LI("begin to rollback snapshot in dsware: %s"),
                 snapshot['volume_id'])
        admin_context = cinder_context.get_admin_context()
        volume = self.db.volume_get(admin_context, snapshot['volume_id'])
        volume_id = self._get_dsware_volume_name(volume)
        snapshot_id = self._get_dsware_snap_name(snapshot)
        self._rollback_snapshot(snapshot_id, volume_id)

    def _calculate_pool_info(self, pool_sets):
        pools_status = []
        reserved_percentage = self.configuration.reserved_percentage

        for pool_info in pool_sets:
            pool = {'pool_name': pool_info['pool_id'],
                    'total_capacity_gb': float(pool_info['total_capacity']) / 1024,
                    'provisioned_capacity_gb': float(pool_info['alloc_capacity']) / 1024,
                    'free_capacity_gb': (float(pool_info['total_capacity']) - float(pool_info['used_capacity'])) / 1024,
                    'QoS_support': True,
                    'multiattach': True,
                    'reserved_percentage': reserved_percentage,
                    'thin_provisioning_support': True,
                    'max_over_subscription_ratio': self.pool_ratio_dict.get(pool_info['pool_id'])}
            pools_status.append(pool)

        return pools_status

    def _update_pool_info_status(self):
        status = {'volume_backend_name': self.configuration.volume_backend_name,
                  'vendor_name': 'Open Source',
                  'driver_version': self.VERSION,
                  'storage_protocol': 'dsware',
                  'total_capacity_gb': 0,
                  'free_capacity_gb': 0,
                  'reserved_percentage': self.configuration.reserved_percentage,
                  'QoS_support': True,
                  'multiattach': True,
                  'thin_provisioning_support': True,
                  'max_over_subscription_ratio': float(self.configuration.over_ratio[0])}

        pool_id = 0
        pool_info = self.dsware_client.query_pool_info(pool_id)
        result = pool_info['result']
        if result == 0:
            status['total_capacity_gb'] = float(
                pool_info['total_capacity']) / 1024
            status['free_capacity_gb'] = (float(
                pool_info['total_capacity']) - float(
                pool_info['used_capacity'])) / 1024
            status['provisioned_capacity_gb'] = float(
                pool_info['alloc_capacity']) / 1024
            LOG.debug("total_capacity_gb is %s, free_capacity_gb is %s",
                      status['total_capacity_gb'],
                      status['free_capacity_gb'])
            self._stats = status
        else:
            self._stats = None

    def _update_pool_id_list_status(self):
        status = {'volume_backend_name': self.configuration.volume_backend_name, 'vendor_name': 'Open Source', 'driver_version': self.VERSION,
                  'storage_protocol': 'dsware'}

        pool_sets = self.dsware_client.query_pool_id_list(self.pool_id_list)
        if not pool_sets:
            self._stats = None
        else:
            pools_status = self._calculate_pool_info(pool_sets)
            status['pools'] = pools_status
            self._stats = status

    def get_volume_stats(self, refresh=False):
        if refresh:
            # old version
            if self.dsware_version == 1:
                self._update_pool_info_status()
            # new version
            elif self.dsware_version == 0:
                self._update_pool_id_list_status()
            else:
                LOG.error(_LE("query dsware version failed!"))
                msg = _("DSWARE query dsware version failed!")
                raise exception.VolumeBackendAPIException(data=msg)

        return self._stats

    def _check_volume_is_assoc_qos(self, volume, volume_type=None):
        """
        check if the volume has been associated with a Qos policy
        :param volume:
        :return: is -- True
                  not -- False
        """
        if not volume_type:
            type_id = volume['volume_type_id']
            if type_id:
                ctxt = context.get_admin_context()
                volume_type = volume_types.get_volume_type(ctxt, type_id)
            else:
                return False

            if not volume_type:
                LOG.info(_LI('dsware cannot get volume volume_type by volume %s'),
                         str(volume))
                return False

            qos_specs_id = volume_type.get('qos_specs_id')
        else:
            qos_specs_id = volume_type.get('qos_specs_id')

        if not qos_specs_id:
            LOG.info(_LI('dsware cannot get qos specs id by volume %s'),
                     str(volume))
            return False

        qos_spec = self._get_qos_specs(qos_specs_id)

        if qos_spec['result'] == 'not support':
            LOG.error(_LE("dsware cannot get qos specs according to qos id"))
            return False
        return True

    def _update_QoS_extend_volume(self, volume, new_size):
        """
        update volume QoS after extend volume size
        :param volume:
        :param new_size:
        :return: 0 -- success
                 1 -- fail
        """
        # check if the volume_type associated with qos
        if not self._check_volume_is_assoc_qos(volume):
            LOG.info(_LI("the volume is not associated with a QoS policy"))
            return 0
        # get original QoS
        original_qos_info = self.query_volume_qos(volume)

        if original_qos_info['result'] != 0:
            LOG.error(_LE("extend_volume(query_volume_qos) failed! result = %s"), original_qos_info['result'])
            return 1

        # calculate new qos info
        qos_specs_id = self._get_volume_qos_id(volume)
        if not qos_specs_id:
            LOG.error(_LE("DSWARE get qos_specs_id failed!"))
            return 1
        qos_info = self._calc_qos(new_size, qos_specs_id)
        qos_info['qos_name'] = original_qos_info.get("qos_name")

        if qos_info['result'] == "not support":
            LOG.error(_LE("extend_volume(query_volume_qos) failed! result = %s"), original_qos_info['result'])
            return 1

        self._check_and_update_qos(qos_info)

        # don't need to update volume QoS
        if qos_info['max_iops'] == original_qos_info['max_iops'] \
                and qos_info['max_mbps'] == original_qos_info['max_mbps']:
            return 0

        return self.dsware_client.update_qos(qos_info)

    def extend_volume(self, volume, new_size):
        # extend volume in dsware
        # two results:(1)extend successfully
        #             (2)any other results would be exception
        dsware_volume_name = self._get_dsware_volume_name(volume)
        LOG.info(_LI("begin to extend volume in dsware: %s"),
                 dsware_volume_name)
        if volume['size'] > new_size:
            msg = _("DSWARE extend Volume failed!"
                    "New size should be greater than old size!")
            raise exception.VolumeBackendAPIException(data=msg)
        # change GB to MB
        volume_size = new_size * 1024

        # Extend volume task will not be scheduled by cinder-scheduler,
        # here check whether the extended volume size will lead to
        # the provisioned_capacity_gb exceeds total_capacity_gb * over_ratio.
        pool_id = self._get_poolid_from_host(volume['host'])
        if self.dsware_version == 1:
            over_ratio = float(self.configuration.over_ratio[0])
        else:
            over_ratio = self.pool_ratio_dict.get(str(pool_id))
        pool_info = self.dsware_client.query_pool_info(pool_id)
        if pool_info['result'] == 0:
            total_capacity_gb = float(
                pool_info['total_capacity']) / 1024
            provisioned_capacity_gb = float(
                pool_info['alloc_capacity']) / 1024
            if (provisioned_capacity_gb + new_size - volume['size']) > (
                    total_capacity_gb * over_ratio):
                LOG.error(_LE("Pool %s has not enough capacity "
                              "to extend volume."),
                          pool_id)
                msg = _("DSWARE extend volume failed, "
                        "for capacity is not enough")
                raise exception.VolumeBackendAPIException(data=msg)
        else:
            LOG.error(_LE("%(pool_id)s query fail, result is %(result)s"),
                      {'pool_id': pool_id,
                       'result': pool_info['result']})
            msg = _("DSWARE query pool failed while extending Volume")
            raise exception.VolumeBackendAPIException(data=msg)

        provider_location = volume.get('provider_location')
        if provider_location:
            provider_location = json.loads(provider_location)
            if provider_location.get('offset') and \
                    provider_location['offset'] == 4:
                volume_size += 1

        result = self.dsware_client.extend_volume(dsware_volume_name,
                                                  volume_size)
        if result != 0:
            msg = _("DSWARE extend Volume failed! %s") % result
            raise exception.VolumeBackendAPIException(data=msg)

        # update volume qos
        if self._check_qos_type(volume) is True:
            result = self._update_QoS_extend_volume(volume, new_size)
            if result != 0:
                msg = _("DSWARE extend Volume(update_QoS) failed! %s") % result
                raise exception.VolumeBackendAPIException(data=msg)

    def clear_download(self, context, volume):
        # handle the situation when copy image to volume stuck
        # detach the volume from the host
        dsware_volume_name = self._get_dsware_volume_name(volume)
        if self._check_quick_start(volume.get('id')):
            host_id = socket.gethostname()
            try:
                dsw_manager_ip = self.dsware_client.get_manage_ip()
                snap_remained = self.db.link_clone_templates_get_all_by_host(
                    context,
                    {'host': host_id, 'status': 'downloading'})
                if snap_remained is not None:
                    LOG.debug(("[DSW-DRIVER] [%s] downloading snapshots "
                               "remained on host [%s] ,"
                               "delete these following")
                              % (len(list(snap_remained)), host_id))

                    for snap_tmp in snap_remained:
                        self.db.link_clone_templates_destroy(context,
                                                             snap_tmp['id'])

                        if str(snap_tmp['is_template']).lower() == 'true':
                            self._delete_template_vol_remained(snap_tmp,
                                                               dsw_manager_ip)
                        self._delete_snapshot(snap_tmp['snap_name'])
            except Exception as e:
                LOG.error(_LE("[DSW-DRIVER] clear remained downloading "
                              "snapshot failed, %s" % e))
        else:
            try:
                dsw_manager_ip = self.dsware_client.get_manage_ip()

                self._dmsetup_remove(dsware_volume_name)
                volume_detach_result = self._dsware_detach_volume(dsware_volume_name,
                                                                  dsw_manager_ip)

                # 50151601:the volume is not attached at the beginning
                # 50151404:the volume does not exist at the beginning
                normal_ret_code = [0, 50151601, 50151404]
                if volume_detach_result is not None \
                        and int(volume_detach_result['ret_code']) not in \
                        normal_ret_code:
                    msg = (_("DSware detach volume from host failed: %s") %
                           volume_detach_result['ret_desc'])
                    LOG.error(msg)
                    # raise exception.VolumeBackendAPIException(data=msg)
            except Exception as err:
                LOG.error(_LE("DSware detach volume from host error:%(err)s"),
                          {'err': err})

    @staticmethod
    def clear_tmp_image(context, volume_id):
        """Clean up after an interrupted image copy.
        :param context: object
        :param volume_id:
        """
        pattern = re.compile(volume_id)
        tmp_file_path = CONF.image_conversion_dir
        try:
            tmp_file_list = list(os.walk(tmp_file_path))[0][2]
            for tmp_file in tmp_file_list:
                if pattern.match(tmp_file):
                    os.unlink(tmp_file_path + '/' + tmp_file)
                    LOG.info(_LI("clear temporary image file : %s"), tmp_file)
            return
        except Exception as err:
            LOG.error(_LE("clear temporary image file error:%(err)s"),
                      {'err': err})

    def _get_data_src_url(self, image_meta, cmk_id, is_encrypted):
        properties = image_meta.get('properties', None)
        source_type = properties.get('__image_source_type', '')
        if source_type == 'obs':
            source_type = 'uds'
        disk_format = image_meta['disk_format']
        if str(CONF.uds_https) == '1':
            http_method = 'https'
        else:
            http_method = 'http'
        ip, port, bucket_name, object_name = \
            properties['__image_location'].split(':')
        # access_key to UDS
        access_key = CONF.s3_store_access_key_for_cinder
        # secret_key to UDS
        secret_key = CONF.s3_store_secret_key_for_cinder
        cmkid_in_data_src_url = cmk_id if cmk_id is not None else ""
        data_src_url = {'source': source_type,
                        'format': disk_format,
                        'proto': http_method,
                        'ip': ip,
                        'port': port,
                        'bucket': bucket_name,
                        'object': object_name,
                        'ak': access_key,
                        'sk': secret_key}
        if is_encrypted is not None and str(is_encrypted) == '1':
            data_src_url.update({'encrypt': is_encrypted,
                                 'vk': properties.get('__system__dek', ''),
                                 'cmkid': cmkid_in_data_src_url})
        data_src_url = json.dumps(data_src_url, separators=(',', ':'))
        return data_src_url

    def _get_model_lld_update(self, volume, dsw_manager_ip, pool_id,
                              dsware_volume_name, provider_location, volume_size):
        offset = 0
        meta_data = volume.get('metadata')
        is_encrypted = None
        cmk_id = None
        if meta_data:
            if CONF.IS_FC:
                # FC Driver Volume
                meta_data['StorageType'] = 'FC_DSWARE'
                meta_data['volInfoUrl'] = 'fusionstorage://' + str(
                    dsw_manager_ip) + '/' + str(
                    pool_id) + '/' + dsware_volume_name
                if 'hw:passthrough' in meta_data:
                    hw_passthrough = meta_data.get('hw:passthrough', None)
                    if hw_passthrough and str(hw_passthrough).lower() == 'true':
                        provider_location['offset'] = 0
                    else:
                        provider_location['offset'] = 4
            else:
                meta_data.update({'manager_ip': dsw_manager_ip,
                                  'StorageType': 'FusionStorage'})
            if 'StorageType' in meta_data:
                provider_location['storage_type'] = meta_data['StorageType']
            if provider_location['offset'] == 4:
                offset = 4096
                volume_size += 1
            if '__system__encrypted' in meta_data:
                is_encrypted = meta_data.get('__system__encrypted', None)
            if '__system__cmkid' in meta_data:
                cmk_id = meta_data.get('__system__cmkid', None)
        model_lld_update = {"metadata": meta_data,
                            "provider_location": json.dumps(provider_location)}
        return model_lld_update, is_encrypted, cmk_id, offset

    def create_LLD_volume(self, volume, image_meta):
        """create lazy loading volume"""
        dsware_volume_name = self._construct_dsware_volume_name(volume)
        dsw_manager_ip = self.dsware_client.get_manage_ip()
        pool_id = self._get_poolid_from_host(volume['host'])
        # G to M
        volume_size = volume['size'] * units.Ki
        provider_location = {'offset': 0, 'storage_type': 'FusionStorage',
                             'ip': dsw_manager_ip, 'pool': pool_id,
                             'vol_name': dsware_volume_name}
        volume_auth_token = volume._context.auth_token

        model_lld_update, is_encrypted, cmk_id, offset = \
            self._get_model_lld_update(
                volume, dsw_manager_ip, pool_id, dsware_volume_name,
                provider_location, volume_size)
        cache_flag = 1
        replace = 0
        is_thin = self.configuration.dsware_isthin
        min_disk = image_meta.get('min_disk')
        # G to M
        image_size = min_disk * 1024
        properties = image_meta.get('properties', None)
        image_location = properties['__image_location']
        check_sum = image_meta.get("checksum", "")
        image_id_string = image_meta['id'] + '_' + image_location + '_' + str(
            check_sum)

        data_src_url = self._get_data_src_url(image_meta, cmk_id, is_encrypted)
        self._create_LLD_volume(
            dsware_volume_name, pool_id, volume_size, is_thin, image_id_string,
            data_src_url, image_size, offset, cache_flag, is_encrypted, cmk_id,
            volume_auth_token, replace)

        replication_driver_data = {'ip': dsw_manager_ip, 'pool': pool_id,
                                   'vol_name': dsware_volume_name}
        model_lld_update['replication_driver_data'] = \
            json.dumps(replication_driver_data)

        # create volume qos.
        self._create_and_associate_qos_for_volume(volume)

        return model_lld_update

    def _create_LLD_volume(self, volume_name, pool_id, volume_size, isThin,
                           image_id, dataSrcUrl, image_size, offset,
                           cacheFlag, is_encrypted, cmkId, volume_auth_token,
                           replace):
        """invoke dsware api to create lazyloading volume"""
        try:
            result = self.dsware_client.create_LLD_volume(
                volume_name, pool_id, volume_size, int(isThin), image_id,
                dataSrcUrl, image_size, offset, cacheFlag, is_encrypted,
                cmkId, volume_auth_token, replace)
        except Exception as e:
            LOG.error(_LE("create volume error, details: %s"), e)
            raise e

        if result != 0:
            msg = _("DSWARE Create LazyLoading Volume failed!"
                    "Result:%s") % result
            raise exception.VolumeBackendAPIException(data=msg)

    def get_lazyloading_count(self, context, identity_type,
                              identity_properties):
        """query the number of lazy loading volume the image is still using
        :param context: object
        """
        identityString = None
        if identity_type == "raw_identity":
            identityString = identity_properties.get("identityString")
        elif identity_type == "image":
            image_meta = identity_properties.get("image_meta")
            properties = image_meta.get('properties', None)
            image_location = properties['__image_location']
            imageRef = identity_properties.get('imageRef')
            check_sum = image_meta.get("checksum", "")
            identityString = imageRef + '_' + image_location + '_' + check_sum
        else:
            LOG.error(_LE("identity_type do not support. "
                          "identity_type is %s"), identity_type)

        try:
            result = self.dsware_client.get_lazyloading_count(identityString)
        except Exception as e:
            LOG.error(_LE("query lazy loading volume number error, "
                          "details: %s"), e)
            raise e
        if result == -1:
            msg = "query lazy loading volume number failed"
            raise exception.VolumeBackendAPIException(data=msg)
        LOG.info(_LI("lazy loading result is %s"), result)
        return result

    def _check_quick_start(self, volume_id):
        """check quick start sign in volume_admin_metadata"""
        admin_context = cinder_context.get_admin_context()
        admin_metadata = self.db.volume_admin_metadata_get(
            admin_context, volume_id)
        quick_start = admin_metadata.get('__quick_start')
        if quick_start is not None and str(quick_start).lower() == 'true':
            return True
        else:
            return False

    def _quick_create_volume(self, context, volume_ref, image_service,
                             image_id, min_disk):
        """quick create volume from template snapshot and master snapshot"""
        dsw_manager_ip = self.dsware_client.get_manage_ip()
        pool_id = self._get_poolid_from_host(volume_ref['host'])
        backend_type = 'DSWARE@%s#%s' % (dsw_manager_ip, pool_id)

        retry_times = 0
        master_snap = None
        # retry_times in CONF
        while retry_times < self.configuration.quickstart_retry_times:
            master_snap = self._get_available_master(context,
                                                     image_id,
                                                     backend_type)
            if master_snap is None:
                master_snap = self._extend_master(context, volume_ref,
                                                  image_service, image_id,
                                                  min_disk, pool_id,
                                                  backend_type)

            max_link_num = self.configuration.quickstart_max_link_num
            success = \
                self.db.link_clone_templates_increase_count(context,
                                                            master_snap['id'],
                                                            max_link_num)
            if success:
                break
            retry_times += 1

        LOG.debug("[DSW-DRIVER] find one master to create volume: %s",
                  master_snap['id'])
        return self._create_volume_from_master(volume_ref, master_snap,
                                               pool_id, min_disk)

    def _get_available_master(self, context, image_id, backend_type):
        # query available master snapshot
        max_link_count = self.configuration.quickstart_max_link_num
        master_result = self.db.link_clone_templates_get_all_by_master(
            context, {'image_id': image_id,
                      'backend_type': backend_type,
                      'is_template': False,
                      'status': 'available'})
        if master_result and master_result[0] and \
                master_result[0]['link_count'] < max_link_count:
            return master_result[0]
        return None

    def _acquire_template_snapshot(self, template_lock, context, volume_ref,
                                   image_service, image_id, min_disk,
                                   pool_id, host, backend_type):
        # acquire template lock and get template snapshot
        try:
            template_lock.acquire(self.configuration.quickstart_lock_timeout)
            LOG.debug(("[DSW-DRIVER] distributed template lock "
                       "of image [%s] acquired by host [%s]")
                      % (image_id, host))
            template_snap, create_new_flag = self._get_template_snapshot(
                context, volume_ref, image_service, image_id, min_disk,
                pool_id, host, backend_type)
            template_lock.release()

            LOG.debug(("[DSW-DRIVER] distributed template lock "
                       "of image [%s] released by host [%s]")
                      % (image_id, host))
        except Exception as e:
            template_lock.release()
            coordinator.stop()
            LOG.error(_LE("[DSW-DRIVER] get template snapshot "
                          " of image [%s] failed")
                      % image_id)
            raise e
        return template_snap, create_new_flag

    def _extend_master(self, context, volume_ref, image_service,
                       image_id, min_disk, pool_id, backend_type):
        host = socket.gethostname()

        connection = CONF.database.connection
        coordinator = coordination.get_coordinator(connection,
                                                   "cinder-dsw-template")
        coordinator.start()

        template_lock = coordinator.get_lock("template-%s" % image_id)
        master_lock = coordinator.get_lock("master-%s-%s" % (pool_id,
                                                             image_id))

        # acquire template lock and get template snapshot
        template_snap, create_new_flag = self._acquire_template_snapshot(
            template_lock, context, volume_ref, image_service, image_id,
            min_disk, pool_id, host, backend_type)

        # acquire master lock and get master snapshot
        try:
            master_lock.acquire(self.configuration.quickstart_lock_timeout)
            LOG.debug("[DSW-DRIVER] distributed master lock "
                      "of image [%s] acquired by host [%s]"
                      % (image_id, host))
            master_snap = self._get_master_snapshot(
                context, image_id, min_disk, pool_id, template_snap, host,
                backend_type, create_new_flag)
            master_lock.release()
            coordinator.stop()
            LOG.debug(("[DSW-DRIVER] distributed master lock "
                       "of image [%s] released by host [%s]")
                      % (image_id, host))
            return master_snap
        except Exception as e:
            master_lock.release()
            coordinator.stop()
            LOG.error(_LE("[DSW-DRIVER] get master snapshot "
                          "of image [%s] failed")
                      % image_id)
            raise e

    def _create_raw_volume(self, raw_uuid, min_disk, volume_ref, image_service,
                           context, template_snap_uuid):
        # create raw volume
        raw_vol_name = 'template-vol-%s' % raw_uuid
        volume_size = min_disk * 1024
        offset = 0
        if CONF.IS_FC:
            meta_data = volume_ref['metadata']
            if 'hw:passthrough' in meta_data:
                hw_passthrough = meta_data.get('hw:passthrough', None)
                if hw_passthrough and str(hw_passthrough).lower() == 'true':
                    offset = 0
                else:
                    offset = 4

            if offset != 0:
                volume_size += 1

        is_thin = self.configuration.dsware_isthin
        try:
            # create a encrypted volume if it is a encrypted image
            image_meta = image_service.show(context, image_id)
            properties = image_meta.get('properties', None)
            is_encrypted = None
            volume_cmkId = None
            if properties is not None:
                is_encrypted = properties.get('__system__encrypted', None)
                volume_cmkId = properties.get('__system__cmkid', None)
            self._create_volume(raw_vol_name, volume_size,
                                is_thin, volume_ref['host'],
                                is_encrypted, volume_cmkId,
                                volume_ref._context.auth_token)
        except Exception as e:
            self.db.link_clone_templates_destroy(context,
                                                 template_snap_uuid)
            LOG.error(_LE("[DSW-DRIVER] create template volume from "
                          "image failed"))
            raise e

    def _create_clone_templates_snapshot(
            self, image_id, min_disk, backend_type, host, volume_ref,
            image_service, context):
        # create new template snapshot if not found in db
        LOG.info(_LI("[DSW-DRIVER] template snapshot of "
                     "image [%s] do not exist in cloning volume, "
                     "new template begins") % image_id)
        template_snap_uuid = str(uuid.uuid4())
        template_snap_name = 'template-snap-%s' % template_snap_uuid
        self.db.link_clone_templates_create(
            context, template_snap_uuid, template_snap_name, min_disk,
            backend_type, host, image_id, True, 'downloading', 0)

        raw_uuid = template_snap_uuid
        self._create_raw_volume(
            raw_uuid, min_disk, volume_ref, image_service, context,
            template_snap_uuid)

        # copy image to new raw volume
        raw_volume_ref = {'id': raw_uuid, 'size': min_disk,
                          'host': volume_ref['host'], 'name': raw_vol_name,
                          'volume_metadata': {}}
        # volume_name is acquired from provider_location in volume object
        provider_location = {'vol_name': raw_vol_name, 'offset': offset}
        raw_volume_ref['provider_location'] = json.dumps(provider_location)
        try:
            self.copy_image_to_volume(context, raw_volume_ref, image_service,
                                      image_id)
        except Exception as e:
            LOG.error(_LE("copy image to volume failed, exception:%s"), e)
            self.db.link_clone_templates_destroy(context, template_snap_uuid)
            self._delete_volume(raw_volume_ref['name'])
            LOG.error(_LE("[DSW-DRIVER] copy image to raw volume failed"))
            raise e

        try:
            self._create_snapshot(template_snap_name, raw_volume_ref['name'])
        except Exception as e:
            self.db.link_clone_templates_destroy(context, template_snap_uuid)
            LOG.error(_LE("create template snapshot from raw volume failed"))
            raise e
        finally:
            LOG.debug(("[DSW-DRIVER] create template snapshot finished,"
                       "delete the raw volume [%s]") % raw_volume_ref['name'])
            try:
                self._delete_volume(raw_volume_ref['name'])
            except Exception as e:
                LOG.error(_LE("[DSW-DRIVER] delete volume failed, %s" % e))

    def _get_template_snapshot(self, context, volume_ref,
                               image_service, image_id, min_disk,
                               pool_id, host, backend_type):
        """get template_snapshot of this image_id"""

        template_find_result = None
        # query template snapshot in db
        template_get_all_result = self.db.link_clone_templates_get_all_by_template(
            context, {'image_id': image_id, 'is_template': True,
                      'status': 'available'})

        if template_get_all_result is not None:
            template_find_result = self._get_dsware_template(
                template_get_all_result)

        if template_find_result:
            LOG.info(_LI("[DSW-DRIVER] template snapshot [%(template_snap)s]"
                         " of image [%(image_id)s] exists in cloning volume")
                     % {'template_snap': template_find_result['id'],
                        'image_id': image_id})
            return template_find_result, False
        else:
            self._create_clone_templates_snapshot(
                image_id, min_disk, backend_type, host, volume_ref,
                image_service, context)

            # update template snapshot status in db
            result = self.db.link_clone_templates_update(
                context,
                template_snap_uuid,
                {'status': 'available'})
            return result, True

    def _get_dsware_template(self, template_get_all):
        """get dsware template of db query result"""
        dsware_template = None
        dsw_manager_ip = self.dsware_client.get_manage_ip()
        if len(list(template_get_all)) > 0:
            for template_tmp in template_get_all:
                template_back = template_tmp['backend_type'].split('#')[0]
                if cmp(template_back, 'DSWARE@%s' % dsw_manager_ip) == 0:
                    dsware_template = template_tmp
                    break
        return dsware_template

    def _create_link_clone_templates(
            self, context, other_pool_list, min_disk, host, image_id):
        dsw_manager_ip = self.dsware_client.get_manage_ip()
        for other_pool_id in other_pool_list:
            pool_backend_type = 'DSWARE@%s#%s' % \
                                (dsw_manager_ip, other_pool_id)
            other_pool_worker = 0
            # the item in self.pool_id_list is string
            other_pool_master_snapshots[other_pool_id] = []
            while other_pool_worker < \
                    self.configuration.quickstart_create_master_num:
                master_id = str(uuid.uuid4())
                master_snapshot_name = 'master-snap-%s' % master_id
                self.db.link_clone_templates_create(
                    context, master_id,
                    master_snapshot_name,
                    min_disk,
                    pool_backend_type,
                    host, image_id, False,
                    'downloading', 0)
                other_pool_master_snapshots[other_pool_id].append(
                    master_snapshot_name)
                other_pool_worker += 1

    def _get_master_snapshot(self, context, image_id, min_disk, pool_id,
                             template_snap, host, backend_type,
                             create_new_flag):
        """get master snapshot of this image_id"""
        # query available master snapshot
        master_result = self._get_available_master(context, image_id,
                                                   backend_type)
        if master_result:
            LOG.debug(("[DSW-DRIVER] master snapshot %s  "
                       "of image %s exists in cloning volume")
                      % (master_result['id'], image_id))
            return master_result
        else:
            # create N master at once, default N = 1
            # create new  master snapshot if not found in db
            LOG.info(_LI("[DSW-DRIVER] master snapshot of "
                         "image %s do not exist in cloning volume, "
                         "new master begins") % image_id)
            worker = 0
            master_snapshot_name_list = []
            while worker < self.configuration.quickstart_create_master_num:
                master_id = str(uuid.uuid4())
                master_snapshot_name = 'master-snap-%s' % master_id
                self.db.link_clone_templates_create(context, master_id,
                                                    master_snapshot_name,
                                                    min_disk, backend_type,
                                                    host, image_id,
                                                    False, 'downloading', 0)
                master_snapshot_name_list.append(master_snapshot_name)
                worker += 1

            # add for create master snap in other pools
            other_pool_list = []
            other_pool_master_snapshots = {}
            if self.configuration.quickstart_create_master_snap_in_all_pool \
                    and create_new_flag:
                other_pool_list = list(self.pool_id_list)
                other_pool_list.remove(str(pool_id))
                self._create_link_clone_templates(
                    context, other_pool_list, min_disk, host, image_id)

            template_snapshot_name = template_snap['snap_name']

            # need to destroy the db entry when create master_snap fail
            # inside _create_snapshot_from_snapshot func.
            return self._create_snapshot_from_snapshot(
                    context, template_snapshot_name, master_snapshot_name_list,
                    pool_id, other_pool_list, other_pool_master_snapshots)

    def _get_model_quick_update_info(self, dsw_manager_ip, volume_ref, pool_id,
                                     dsware_volume_name, volume_size):
        meta_data = volume_ref['metadata']
        provider_location = {}
        if CONF.IS_FC:
            # FC Driver Volume
            meta_data['StorageType'] = 'FC_DSWARE'
            meta_data['volInfoUrl'] = 'fusionstorage://' + str(
                dsw_manager_ip) + '/' + str(
                pool_id) + '/' + dsware_volume_name
            hw_passthrough = meta_data.get('hw:passthrough', None)
            if hw_passthrough and str(hw_passthrough).lower() == 'true':
                provider_location['offset'] = 0
            else:
                provider_location['offset'] = 4
        else:
            meta_data.update({'manager_ip': dsw_manager_ip,
                              'StorageType': 'FusionStorage'})
            provider_location['offset'] = 0

        provider_location['storage_type'] = meta_data['StorageType']
        provider_location['ip'] = dsw_manager_ip
        provider_location['pool'] = pool_id
        provider_location['vol_name'] = dsware_volume_name
        model_quick_update = {"metadata": meta_data,
                              "provider_location":
                                  json.dumps(provider_location)}
        if provider_location['offset'] == 4:
            # When volume_ref['size'] is large than min_disk. We also need
            # to extend 1M siz, for when nova attach the volume, the dm
            # device will be created based on the volume_ref['size']
            volume_size += 1
        return model_quick_update

    def _create_volume_from_master(self, volume_ref, master, pool_id,
                                   min_disk):
        master_snap_ref = {'id': master['id'],
                           'name': master['snap_name'],
                           'volume_size': master['size']}
        context = cinder_context.get_admin_context()

        try:
            dsware_volume_name = self._construct_dsware_volume_name(volume_ref)
            volume_size = volume_ref['size'] * 1024
            dsw_manager_ip = self.dsware_client.get_manage_ip()
            model_quick_update = self._get_model_quick_update_info(
                dsw_manager_ip, volume_ref, pool_id, dsware_volume_name,
                volume_size)

            self._create_volume_from_snap(dsware_volume_name,
                                          volume_size,
                                          master_snap_ref['name'])

            replication_driver_data = {'ip': dsw_manager_ip,
                                       'pool': pool_id,
                                       'vol_name': dsware_volume_name}
            model_quick_update['replication_driver_data'] = json.dumps(
                replication_driver_data)
        except Exception as e:
            # update link count
            self.db.link_clone_templates_decrease_count(context, master['id'])
            # delete master if count equal zero
            if self.configuration.quickstart_delete_master:
                self._delete_master_snap(context, master['id'])
            LOG.error(_LE("[DSW-DRIVER] create volume from snapshot failed"))
            raise e

        self.db.volume_admin_metadata_update(
            context,
            volume_ref.get('id'),
            {'master_id': master['id']},
            False)

        updates = dict(model_quick_update or dict())

        return updates

    def _create_master_snapshot_from_snap(
            self, context, pool_list, master_snapshots, template_snapshot_name):
        fullCopyFlag = 0
        smartFlag = 0
        create_success_master_snap = {}
        for pool_id in pool_list:
            create_success_master_snap[pool_id] = []
            for master_snapshot_name in master_snapshots[pool_id]:
                result = self.dsware_client.create_snapshot_from_snap(
                    template_snapshot_name, master_snapshot_name, pool_id,
                    fullCopyFlag, smartFlag)
                if result == 0:
                    create_success_master_snap[pool_id].append(master_snapshot_name)
                else:
                    LOG.error(_LE("[DSW-DRIVER] Create "
                                  "master_snapshot:%(master_snapshot)s from "
                                  "template_snapshot_name:%(template_snapshot)s "
                                  "failed")
                              % {'master_snapshot': master_snapshot_name,
                                 'template_snapshot': template_snapshot_name})
                    prefix = len('master-snap-')
                    master_id = master_snapshot_name[prefix:]
                    self.db.link_clone_templates_destroy(context, master_id)

        wait_success_master_snap = {}
        for pool_id in pool_list:
            wait_success_master_snap[pool_id] = []
            for master_snapshot_name in create_success_master_snap[pool_id]:
                ret = self._wait_for_duplicate_snapshot_finish_timer(
                    master_snapshot_name)
                if ret:
                    wait_success_master_snap[pool_id].append(
                        master_snapshot_name)
                else:
                    prefix = len('master-snap-')
                    master_id = master_snapshot_name[prefix:]
                    self.db.link_clone_templates_destroy(
                        context, master_id)
        return wait_success_master_snap

    def _create_snapshot_from_snapshot(self, context,
                                       template_snapshot_name,
                                       master_snapshot_name_list,
                                       pool_id,
                                       other_pool_list,
                                       other_pool_master_snapshots):
        """create master snapshots from template snapshot"""
        other_pool_list.append(pool_id)
        other_pool_master_snapshots[pool_id] = master_snapshot_name_list
        master_snapshots = self._create_master_snapshot_from_snap(
            context, other_pool_list, other_pool_master_snapshots, template_snapshot_name)

        LOG.info(_LI('dsware_client.duplicate_snapshot end'))
        result = None
        for key, value in master_snapshots.items():
            for master_snapshot in value:
                prefix = len('master-snap-')
                master_id = master_snapshot[prefix:]
                self.db.link_clone_templates_update(
                    context, master_id, {'status': 'available'})
        if result is None:
            LOG.error(_LE("[DSW-DRIVER] All master snapshot "
                          "create fail in pool %s"),
                      pool_id)
            msg = _('all master_snapshots failed')
            raise exception.VolumeBackendAPIException(data=msg)
        return result

    def _wait_for_duplicate_snapshot_finish_timer(self, dst_snap_name):
        """wait duplicate snapshot finish timer"""
        timer = loopingcall.FixedIntervalLoopingCall(
            self._check_duplicate_snapshot_finish, dst_snap_name)
        LOG.debug('Calling _check_duplicate_snapshot_finish: volume-name %s'
                  % dst_snap_name)
        self.quickstart_master_snap_count = 0
        ret = timer.start(interval=self.check_cloned_interval).wait()
        timer.stop()
        return ret

    def _check_duplicate_snapshot_finish(self, dst_snap_name):
        """check duplicate snapshot finish"""
        LOG.debug('Loop call: _check_duplicate_snapshot_finish(), '
                  'dst_snap_name %s' % dst_snap_name)
        current_snapshot = self.dsware_client.query_snap(dst_snap_name)

        if current_snapshot:
            status = current_snapshot['status']
            LOG.debug('wait duplicated snapshot,{0} {1}'.format(dst_snap_name,
                                                                status))
            if int(status) == self.DSWARE_SNAP_CREATING_STATUS:
                self.quickstart_master_snap_count += 1
            elif int(status) == self.DSWARE_SNAP_CREATE_SUCCESS_STATUS:
                raise loopingcall.LoopingCallDone(retvalue=True)
            else:
                msg = (_('duplicated snapshot failed, ret_code %s')
                       % status)
                LOG.error(msg)
                raise loopingcall.LoopingCallDone(retvalue=False)
            if self.quickstart_master_snap_count > self.configuration. \
                    quickstart_clone_snapshot_timeout:
                msg = (_('DSWARE duplicated snapshot [%s] time out')
                       % dst_snap_name)
                LOG.error(msg)
                raise loopingcall.LoopingCallDone(retvalue=False)
        else:
            LOG.warning(
                (_('Can not find snapshot %s from dsware') % dst_snap_name))
            self.quickstart_master_snap_count += 1
            if self.quickstart_master_snap_count > 10:
                msg = "DSWARE duplicate snapshot failed: snapshot" \
                      "can not find from dsware"
                LOG.error(msg)
                raise loopingcall.LoopingCallDone(retvalue=False)

    def _quick_delete_volume(self, volume):
        """delete volume created from template snapshot and master snapshot"""
        # get master snapshot id
        context = cinder_context.get_admin_context()
        admin_metadata = self.db.volume_admin_metadata_get(
            context, volume['id'])
        master_id = None
        if 'master_id' in admin_metadata:
            LOG.debug(("[DSW-DRIVER] master id in volume admin metadata is "
                       "[%s] " % admin_metadata['master_id']))
            master_id = admin_metadata['master_id']
        else:
            volume_name = self._get_dsware_volume_name(volume)
            master_query_result = self.dsware_client. \
                query_volume(volume_name)
            if master_query_result['result'] == 0:
                master_name = master_query_result['father_name']
                ref_len = len('master-snap-')
                master_id = str(master_name[ref_len:])
                LOG.debug(("[DSW-DRIVER] master id queryed in dsware is "
                           "[%s] " % master_id))

        # delete volume
        dsware_volume_name = self._get_dsware_volume_name(volume)
        is_volume_exist = self._delete_volume(dsware_volume_name)
        # Check result, if volume exist and delete successfully,
        # then do the following. If cinder driver return successfully,
        # but cinder manage module fails, and the volume in dsware is deleted,
        # and the count has already been decreased by one. The volume status is
        # error_deleting, when user force delete the volume again, it should
        # not decrease the count again.
        if is_volume_exist:
            # update link count
            self.db.link_clone_templates_decrease_count(context, master_id)
            # delete master if count equal zero, when delete master snap raise
            # exception and do not pass exception to cinder, for the volume
            # has been deleted successfully.
            try:
                if self.configuration.quickstart_delete_master:
                    self._delete_master_snap(context, master_id)
            except Exception as e:
                LOG.error(_LE("[DSW-DRIVER]delete master snap%(master_id) "
                              "error, result:%(result)s")
                          % {'master_id': master_id, 'result': e})

        return True

    def _delete_master_snap(self, context, master_id):
        master_table = self.db.link_clone_templates_get_by_id(context,
                                                              master_id)
        if master_table is None:
            LOG.debug("[DSW-DRIVER] master in db already deleted")
            return True

        # try to delete master if count equal zero
        result = self.db.link_clone_delete_master(context, master_id)
        if result:
            dsw_link_vol_result = self.dsware_client.query_volumes_from_snap(
                master_table['snap_name'])
            if dsw_link_vol_result[0] == 0:
                # to be done:dsw_link_count is correct?
                dsw_link_count = len(dsw_link_vol_result[1])
                if dsw_link_count == 0:
                    self._delete_snapshot(master_table['snap_name'])
                    LOG.info(_LI("[DSW-DRIVER] master snapshot[%s] link count"
                                 " is zero,delete this "
                                 "master snapshot") % master_table['id'])
                else:
                    self.db.link_clone_templates_update(context,
                                                        master_table['id'],
                                                        {'link_count': dsw_link_count,
                                                         "deleted": False})
                    LOG.info(_LI("[DSW-DRIVER] master snapshot[%s] link count "
                                 "failed before update it correctly")
                             % master_table['id'])

    def _delete_template_vol_remained(self, snap_tmp, dsw_manager_ip):
        """delete volume created from downloading template snapshot"""
        template_vol_name = 'template-vol-%s' % snap_tmp['id']
        template_vol_result = self.dsware_client.query_volume(
            template_vol_name)
        if template_vol_result['result'] == 0:
            LOG.info(_LI(
                "[DSW-DRIVER] volume of template snapshot [%s] remained, "
                "detach and delete it following") % snap_tmp['id'])

            self._dmsetup_remove(template_vol_name)
            detach_result = self._dsware_detach_volume(template_vol_name,
                                                       dsw_manager_ip)
            if detach_result is not None \
                    and int(detach_result['ret_code']) != 0:
                LOG.error(_LE(
                    "[DSW-DRIVER] detach template volume "
                    "[%s] from host failed") % template_vol_name)

            delete_result = self.dsware_client.delete_volume(
                template_vol_name)
            if delete_result != 0:
                LOG.error(_LE("[DSW-DRIVER] delete template volume "
                              "[%s] failed") % template_vol_name)
        else:
            LOG.debug("[DSW-DRIVER] no template volume remained")

    def manage_existing(self, volume, existing_ref):
        if existing_ref.get('provider_location'):
            provider_location = json.loads(existing_ref['provider_location'])
            dsware_volume_name = provider_location.get('vol_name')
            result = self._get_volume(dsware_volume_name)
            if result is not True:
                msg = _("DSWARE volume %s not exist!") % dsware_volume_name
                raise exception.VolumeBackendAPIException(data=msg)
            volume_image_meta = existing_ref.get('volume_image_metadata',
                                                 None)
            if volume_image_meta:
                admin_context = cinder_context.get_admin_context()
                self.db.volume_glance_metadata_bulk_create(admin_context,
                                                           volume['id'],
                                                           volume_image_meta)
            return {
                "provider_location": existing_ref['provider_location'],
            }
        else:
            dsware_volume_name = existing_ref['source-name']
            result = self.dsware_client.query_volume(dsware_volume_name)
            if result and result['result'] == 0:
                pool_name = volume_utils.extract_host(volume.host, level='pool')
                if pool_name != result['pool_id'] or \
                        pool_name not in self.pool_id_list:
                    msg = _('volume %(volume)s does not exist on Storage pool '
                            '%(pool)s. Please check.') \
                          % {"volume": dsware_volume_name, "pool": pool_name}
                    LOG.error(msg)
                    raise exception.VolumeBackendAPIException(data=msg)
            else:
                msg = _("DSWARE Query Volume failed! Result:%s") % result
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)
            # wait for the quick_start image_volume to be created successfully.
            ret = self._wait_for_quick_start_volume_finish_interval_one_timer(
                dsware_volume_name)
            if not ret:
                ret = self._wait_quick_start_vol_finish_interval_five_timer(
                    dsware_volume_name)
                if not ret:
                    msg = _('quickstart volume create failed')
                    raise exception.VolumeBackendAPIException(data=msg)

            provider_location = {'offset': 0,
                                 'storage_type': 'FusionStorage',
                                 'ip': self.dsware_client.get_manage_ip(),
                                 'pool': result['pool_id'],
                                 'vol_name': dsware_volume_name}

            volume_image_meta = existing_ref.get('volume_image_metadata',
                                                 None)
            if volume_image_meta:
                admin_context = cinder_context.get_admin_context()
                self.db.volume_glance_metadata_bulk_create(admin_context,
                                                           volume['id'],
                                                           volume_image_meta)
            return {
                "provider_location": json.dumps(provider_location),
            }

    def _wait_for_quick_start_volume_finish_interval_one_timer(self,
                                                               volume_name):
        timer = loopingcall.FixedIntervalLoopingCall(
            self._check_quick_start_volume_finish_interval_one, volume_name)

        self.quickstart_volume_count = 0
        ret = timer.start(interval=self.check_quickstart_interval_one).wait()
        timer.stop()
        return ret

    def _wait_quick_start_vol_finish_interval_five_timer(self,
                                                         volume_name):
        timer = loopingcall.FixedIntervalLoopingCall(
            self._check_quick_start_volume_finish_interval_five, volume_name)

        self.quickstart_volume_count = 0
        ret = timer.start(interval=self.check_quickstart_interval_five).wait()
        timer.stop()
        return ret

    def _check_quick_start_volume_finish_interval_one(self, volume_name):
        LOG.debug('Loop call: _check_quick_start_volume_finish_interval_one(),'
                  'volume-name %s',
                  volume_name)
        current_volume = self.dsware_client.query_volume(volume_name)

        if current_volume and current_volume['status']:
            status = current_volume['status']
            LOG.debug('Wait clone volume %(volume_name)s, status:%(status)s.',
                      {"volume_name": volume_name,
                       "status": status})
            if int(status) == self.DSWARE_VOLUME_CREATING_STATUS or int(
                    status) == self.DSWARE_VOLUME_DUPLICATE_VOLUME:
                self.quickstart_volume_count += 1
            elif int(status) == self.DSWARE_VOLUME_CREATE_SUCCESS_STATUS:
                raise loopingcall.LoopingCallDone(retvalue=True)
            else:
                msg = _('Clone volume %(new_volume_name)s failed, '
                        'volume status is:%(status)s.')
                LOG.error(msg, {'new_volume_name': volume_name,
                                'status': status})
                # raise exception.VolumeDriverException(message=msg)
                raise loopingcall.LoopingCallDone(retvalue=False)
            if self.quickstart_volume_count > \
                    self.configuration.quickstart_interval_one_timeout:
                msg = _('Dsware clone volume time out. '
                        'Volume: %(new_volume_name)s, status: %(status)s')
                LOG.error(msg, {'new_volume_name': volume_name,
                                'status': current_volume['status']})
                # raise exception.VolumeDriverException(message=msg)
                raise loopingcall.LoopingCallDone(retvalue=False)
        else:
            # when the volume is not exist, it will enter here
            LOG.warning(_LW('Can not find volume %s from dsware'),
                        volume_name)
            self.quickstart_volume_count += 1
            if self.quickstart_volume_count > \
                    self.configuration.quickstart_interval_one_timeout:
                msg = _("DSWARE clone volume failed:volume "
                        "can not find from dsware")
                LOG.error(msg)
                raise loopingcall.LoopingCallDone(retvalue=False)

    def _check_quick_start_volume_finish_interval_five(self, volume_name):
        LOG.debug('Loop call: _check_quick_start_volume_finish_interval_five(),'
                  'volume-name %s',
                  volume_name)
        current_volume = self.dsware_client.query_volume(volume_name)

        if current_volume and current_volume['status']:
            status = current_volume['status']
            LOG.debug('Wait clone volume %(volume_name)s, status:%(status)s.',
                      {"volume_name": volume_name,
                       "status": status})
            if int(status) == self.DSWARE_VOLUME_CREATING_STATUS or int(
                    status) == self.DSWARE_VOLUME_DUPLICATE_VOLUME:
                self.quickstart_volume_count += 1
            elif int(status) == self.DSWARE_VOLUME_CREATE_SUCCESS_STATUS:
                raise loopingcall.LoopingCallDone(retvalue=True)
            else:
                msg = _('Clone volume %(new_volume_name)s failed, '
                        'volume status is:%(status)s.')
                LOG.error(msg, {'new_volume_name': volume_name,
                                'status': status})
                raise loopingcall.LoopingCallDone(retvalue=False)
            if self.quickstart_volume_count > \
                    self.configuration.quickstart_interval_five_timeout:
                msg = _('Dsware clone volume time out. '
                        'Volume: %(new_volume_name)s, status: %(status)s')
                LOG.error(msg, {'new_volume_name': volume_name,
                                'status': current_volume['status']})
                # raise exception.VolumeDriverException(message=msg)
                raise loopingcall.LoopingCallDone(retvalue=False)
        else:
            # when the volume is not exist, it will enter here
            LOG.warning(_LW('Can not find volume %s from dsware'),
                        volume_name)
            self.quickstart_volume_count += 1
            if self.quickstart_volume_count > \
                    self.configuration.quickstart_interval_five_timeout:
                msg = _("DSWARE clone volume failed:volume "
                        "can not find from dsware")
                LOG.error(msg)
                raise loopingcall.LoopingCallDone(retvalue=False)

    def manage_existing_get_size(self, volume, existing_ref):
        """
        :param volume: object
        """
        vol_name = existing_ref.get('source-name')
        vol_info = self.dsware_client.query_volume(vol_name)
        if vol_info and vol_info['result'] == 0:
            remainder = float(vol_info.get('vol_size')) % units.Ki
            if remainder != 0:
                msg = _("The volume size must be an integer multiple of 1 GB.")
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)
            size = int(vol_info.get('vol_size')) / units.Ki
            return size
        else:
            msg = _("DSWARE Query Volume failed! Result:%s") % vol_info
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

    def get_pools(self, context, filters=None):
        return self.get_volume_stats(refresh=True)

    def _calc_qos(self, vol_size, qos_specs_id, original_qos_info=None):
        """
        calculate volume qos
        :param vol_size: volume size
        :param qos_specs_id: qos rule
        :return:
        """
        LOG.info(_LI("DSWARE calculate qos, vol_size = %d, \
        qos_specs_id = %s"), vol_size, str(qos_specs_id))

        qos_spec = self._get_qos_specs(qos_specs_id)
        if qos_spec['result'] == 'not support':
            msg = (_("DSWARE  get QoS specs failed! QoS_ID = %s "),
                   str(qos_specs_id))
            raise exception.VolumeBackendAPIException(data=msg)

        qos_info = fspythonapi.qos_info.copy()
        if original_qos_info:
            qos_info['qos_name'] = original_qos_info.get("qos_name")

        if qos_spec['result'] == 'not support':
            qos_info['result'] = 'not support'
            return qos_info

        if 'TOTAL_IOPS_SEC' in qos_spec:
            qos_info['max_iops'] = qos_spec.get('TOTAL_IOPS_SEC')

        if 'TOTAL_BYTES_SEC' in qos_spec:
            total_byte_sec = int(qos_spec.get('TOTAL_BYTES_SEC'))
            if 0 < total_byte_sec < units.Mi:
                total_byte_sec = units.Mi
            qos_info['max_mbps'] = str(total_byte_sec / units.Mi)

        if "MAXIOPS" in qos_spec:
            qos_info['max_iops'] = qos_spec['MAXIOPS']

        if "MAXBANDWIDTH" in qos_spec:
            qos_info['max_mbps'] = qos_spec['MAXBANDWIDTH']

        if "MAXMBPS" in qos_spec:
            qos_info['max_mbps'] = qos_spec['MAXMBPS']

        LOG.info(_LI('dsware create qos info: %s'), str(qos_info))

        return qos_info

    def _check_qos_parameter(self, qos_info):
        if qos_info['max_iops'] == '' or qos_info['max_mbps'] == '':
            msg = (_("DSWARE  qos_info parameter error: "
                     "max_iops or max_mbps is null! %s"),
                   str(qos_info))
            LOG.error(_LE("%s"), msg)
            return False
        return True

    def _real_update_QoS_retype(self, volume, new_type):
        """
        when the volume and the new_type are associated with two different QoS
        :param volume:
        :param new_type:
        :return:
        """
        LOG.info(_LI("Begin to update Volume QoS for volume_retype"))
        # get original qos info
        old_qos_info = self.query_volume_qos(volume)
        vol_size = volume['size']
        # get new qos info
        qos_specs_id = new_type.get('qos_specs_id')
        if not qos_specs_id:
            msg = (_("DSWARE  get qos_type QoS failed! qos_type = %s "),
                   new_type)
            raise exception.VolumeBackendAPIException(data=msg)

        # calculate partial qos info
        qos_info = self._calc_qos(vol_size, qos_specs_id, old_qos_info)
        self._check_and_update_qos(qos_info)

        # update volume qos
        LOG.info(_LI('retype: dsware update qos info for retype: %s'),
                 str(qos_info))
        self.update_qos(qos_info)

    def __create_and_associate_qos_for_retype(self, volume, new_type):
        LOG.info(_LI("Begin to create and associate Volume QoS for \
        volume_retype"))
        try:
            qos_specs_id = new_type.get('qos_specs_id')
            qos = self._create_qos_for_volume(volume, qos_specs_id)
        except Exception as e:
            msg = (_("DSWARE Retype: Create Volume QoS failed! \
            Exception:%(e)s, ") % {'e': e})
            raise exception.VolumeBackendAPIException(data=msg)

        if qos['result'] == 'not support':
            return

        if qos['result'] != 0:
            msg = (_("DSWARE Retype: Create Volume QoS failed!\
             Result:%(qos)d, ") % {'qos': qos['result']})
            raise exception.VolumeBackendAPIException(data=msg)

        dsware_volume_name = self._construct_dsware_volume_name(volume)
        result = self.dsware_client.associate_qos_with_volume(
            qos['qos_name'], dsware_volume_name)
        if result != 0:
            del_qos = self.delete_qos(qos['qos_name'])
            msg = (_("DSWARE: Retype: Create and Associate Qos Failed! Result:"
                     "Clear Qos Result:%(del_qos)d") % {'result': result,
                                                        'del_qos': del_qos})
            raise exception.VolumeBackendAPIException(data=msg)

    def _dsware_disasso_qos(self, qos_name, volume_name):
        try:
            result = self.dsware_client.disassociate_qos_with_volume(
                qos_name, volume_name)
        except Exception as e:
            LOG.error(_LE("Disassociate Qos %(qos)s with volume %(volume)s "
                          "error, details: %(exception)s"),
                      {'qos': qos_name, 'volume': volume_name, 'exception': e})
            raise e

        if result != 0:
            msg = _("DSWARE Disassociate Qos %(qos)s with volume %(volume)s "
                    "failed! Result:%(result)s") % {'qos': qos_name,
                                                    'volume': volume_name,
                                                    'result': result}
            raise exception.VolumeBackendAPIException(data=msg)

    def _check_and_disasso_qos(self, volume):
        """
        check if the volume is associated with a qos in storage only,
        then disassociate
        :param volume:
        :return:
        """
        LOG.info(_LI("DSWARE _check_and_disasso_qos"))
        vol_qos = self.query_volume_qos(volume)
        volume_name = self._construct_dsware_volume_name(volume)
        if vol_qos['result'] != 0:
            msg = _("DSWARE Disassociate Qos with volume %(volume)s Query Qos"
                    " failed! Result:%(vol_qos)s") % {
                      'volume': volume_name,
                      'vol_qos': dict(vol_qos)}
            raise exception.VolumeBackendAPIException(data=msg)

        if vol_qos['para_num'] == 0:
            LOG.info(_LI('dsware disassociate volume%(volume)s qos:%(qos)s, '
                         'not associate qos') % {'volume': str(volume),
                                                 'qos': str(vol_qos)})
            return

        qos_name = vol_qos['qos_name']
        LOG.info(_LI('dsware disassociate qos:%(qos)s, volume%(volume)s'),
                 {'qos': str(qos_name), 'volume': str(volume)})

        if qos_name is not None:
            self._dsware_disasso_qos(qos_name, volume_name)

    def _update_QoS_retype(self, context, volume, new_type):
        """
        update QoS info for retype
        :param context:
        :param volume:
        :param new_type:
        :return:
        """
        is_volume_assoc_qos = self._check_volume_is_assoc_qos(volume)
        is_new_type_assoc_qos = self._check_volume_is_assoc_qos(volume,
                                                                new_type)
        # if the volume is not associated with a Qos and new_type
        # is associated with a QoS, create and associate a new qos
        if not is_volume_assoc_qos and is_new_type_assoc_qos:
            LOG.info(_LI("the volume is not associated with a Qos but\
             new_type is associated with a QoS"))
            # cinder do not associate with a qos,
            # but volume associate with a qos in dsware
            self._check_and_disasso_qos(volume)
            self.__create_and_associate_qos_for_retype(volume, new_type)
            return

        # if the volume is associated with a QoS and the new_type is not
        #  associated with a QoS, then disassociate qos with volume
        if not is_new_type_assoc_qos and is_volume_assoc_qos:
            LOG.info(_LI("the volume is associated with a Qos but\
                         new_type is not associated with a QoS"))
            qos_id = self._get_volume_qos_id(volume)
            self.disassociate_qos_with_volume(qos_id, volume)
            return

        # neither the volume nor the new_type is associated with
        # a QoS, do nothing
        if not is_volume_assoc_qos and not is_new_type_assoc_qos:
            LOG.info(_LI("neither the volume nor the new_type is \
            associated with a QoS"))
            return

        # if the two types are associated with the same QoS,
        # do nothing
        qos_id_new_type = self._get_type_qos_id(new_type)
        qos_id_volume = self._get_volume_qos_id(volume)
        if qos_id_new_type and qos_id_new_type == qos_id_volume:
            LOG.info(_LI("the volume type and the new_type are \
            associated with the same QoS"))
            return

        # if the two types are associated with two different QoS,
        # update volume's QoS
        self._real_update_QoS_retype(volume, new_type)

    def retype(self, context, volume, new_type, diff, host):
        """
        change volume to new type.
        :param context:
        :param volume: the volume to retype
        :param new_type: the new type the volume need to change to
        :param diff:
        :param host: backend of storage
        :return:
        """
        LOG.info(_LI("DSWARE retype: id=%(id)s, new_type=%(new_type)s, "
                     "diff=%(diff)s, host=%(host)s."), {'id': volume['id'],
                                                        'new_type': new_type,
                                                        'diff': diff,
                                                        'host': host})
        is_volume_with_qos = self._check_qos_type(volume)
        is_type_with_qos = self._check_qos_type(None, new_type)
        LOG.info(_LI("is_volume_with_qos %s, is_type_with_qos %s" % (is_volume_with_qos, is_type_with_qos)))
        if is_volume_with_qos is True or is_type_with_qos is True:
            if volume['host'] == host['host']:
                # update volume qos
                try:
                    self._update_QoS_retype(context, volume, new_type)
                except Exception as e:
                    msg = (_("DSWARE Retype: Update Volume QoS failed!\
                     Exception:%(e)s") % {'e': e})
                    raise exception.VolumeBackendAPIException(data=msg)
            else:
                raise exception.InvalidParameterValue(
                    'DSWARE the original type of the volume and\
                     the new type are not in the same pool')

            return True
        else:
            raise NotImplementedError()

    def query_volume_qos(self, volume):
        vol_name = self._get_dsware_volume_name(volume)
        qos_info = self.dsware_client.query_volume_qos(vol_name)

        if qos_info['result'] != 0:
            LOG.info(_LI("Query volume %(vol)s QoS failed! Result %(qos)s.")
                     % {'vol': vol_name, 'qos': str(qos_info)})

        return qos_info

    def _get_qos_specs(self, qos_specs_id):

        LOG.info(_LI("Dsware get qos specs: qos_specs_id = %s"), qos_specs_id)
        qos = FSP_QOS_INFO.copy()

        ctxt = context.get_admin_context()
        consumer = qos_specs.get_qos_specs(ctxt, qos_specs_id)['consumer']
        if consumer != 'back-end':
            qos['result'] = 'not support'
            return qos

        kvs = qos_specs.get_qos_specs(ctxt, qos_specs_id)['specs']

        for k, v in kvs.items():
            if k not in HUAWEI_VALID_KEYS:
                continue
            qos[k.upper()] = v

        for upper_limit in UPPER_LIMIT_KEYS:
            for lower_limit in LOWER_LIMIT_KEYS:
                if upper_limit in qos and lower_limit in qos:
                    msg = (_('QoS policy upper_limit and lower_limit '
                             'conflict, QoS policy: %(qos_policy)s.')
                           % {'qos_policy': qos})
                    LOG.error(msg)
                    raise exception.InvalidInput(reason=msg)

        qos['result'] = 'success'

        LOG.info(_LI('The QoS sepcs is: %s.'), str(qos))
        return qos

    def _check_and_update_qos(self, qos):
        LOG.info(_LI('dsware update qos: %s'), str(qos))
        if type(qos) != dict:
            raise exception.InvalidParameterValue('params type is not dict')

        if not qos['max_iops'] or int(qos['max_iops']) == 0:
            qos['max_iops'] = '999999999'

        if not qos['max_mbps'] or int(qos['max_mbps']) == 0:
            qos['max_mbps'] = '999999'

        burst_iops = qos['burst_iops']
        credit_iops = qos['credit_iops']

        if credit_iops and burst_iops \
                and (credit_iops != '0' and burst_iops != '0'):
            try:
                multipe = int(credit_iops) % int(burst_iops)
                if multipe != 0:
                    raise exception.InvalidParameterValue(
                        'credisIOPS is not  burstIOPS integral multiple')
            except Exception:
                raise exception.InvalidParameterValue(
                    'credisIOPS or burstIOPS is not int')
        elif (not credit_iops and not burst_iops) \
                or (credit_iops == '0' and burst_iops == '0'):
            pass
        # creditIOPS and burstIOPS must exist at the same time
        else:
            raise exception.InvalidParameterValue(
                'credisIOPS and burstIOPS do not exist at the samt time')

        return qos

    def _check_openstack_qos_name(self, qos_info):
        if re.search('^openstack_vol_qos_.*-pool_id-[0-9].*-lun_id-[0-9]',
                     qos_info['qos_name']):
            return True
        else:
            return False

    def _generate_qos_name(self, volume_name):
        volume_info = self.dsware_client.query_volume(volume_name)
        now_time = datetime.datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        qos_name = 'openstack_vol_qos_' + now_time + '-pool_id-' + \
                   str(volume_info.get('pool_id')) + '-lun_id-' + \
                   str(volume_info.get('lun_id'))

        return qos_name

    def _create_qos_info(self, qos_specs_id):
        qos_spec = self._get_qos_specs(qos_specs_id)

        qos_info = fspythonapi.qos_info.copy()
        if qos_spec['result'] == 'not support':
            qos_info['result'] = 'not support'
            return qos_info

        if 'TOTAL_IOPS_SEC' in qos_spec:
            qos_info['max_iops'] = qos_spec.get('TOTAL_IOPS_SEC')

        if 'TOTAL_BYTES_SEC' in qos_spec:
            total_byte_sec = int(qos_spec.get('TOTAL_BYTES_SEC'))
            if 0 < total_byte_sec < units.Mi:
                total_byte_sec = units.Mi
            qos_info['max_mbps'] = str(total_byte_sec / units.Mi)

        if "MAXIOPS" in qos_spec:
            qos_info['max_iops'] = qos_spec['MAXIOPS']

        if "MAXBANDWIDTH" in qos_spec:
            qos_info['max_mbps'] = qos_spec['MAXBANDWIDTH']

        if "MAXMBPS" in qos_spec:
            qos_info['max_mbps'] = qos_spec['MAXMBPS']

        local_qos = self._check_and_update_qos(qos_info)

        LOG.info(_LI('dsware create qos info: %s'), str(local_qos))

        return local_qos

    @staticmethod
    def _get_type_qos_id(type):
        return type.get('qos_specs_id')

    @staticmethod
    def _get_volume_qos_id(volume):
        volume_type = None
        type_id = volume['volume_type_id']
        if type_id:
            ctxt = context.get_admin_context()
            volume_type = volume_types.get_volume_type(ctxt, type_id)

        if not volume_type:
            LOG.info(_LI('dsware cannot get volume type by volume %s'),
                     str(volume))
            return None

        return volume_type.get('qos_specs_id')

    def _create_qos_for_volume(self, volume, qos_id=None):
        LOG.info(_LI('DSWARE _create_qos_for_volume'))

        qos_info = {}
        if not qos_id:
            LOG.info(_LI('dsware create qos without qos id'))
            qos_specs_id = self._get_volume_qos_id(volume)
            if not qos_specs_id:
                LOG.info(_LI('dsware cannot get qos specs id by volume %s'),
                         str(volume))
                qos_info['result'] = 'not support'
                return qos_info
            is_with_qos = self._check_qos_type(volume)
        else:
            LOG.info(_LI('dsware create qos with qos id %s'), str(qos_id))
            qos_specs_id = qos_id
            is_with_qos = self._check_qos_specs(qos_specs_id)

        if is_with_qos is True:
            qos_info = self._calc_qos(volume['size'], qos_specs_id)
            self._check_and_update_qos(qos_info)
        else:
            qos_info = self._create_qos_info(qos_specs_id)

        if qos_info['result'] == 'not support':
            LOG.info(_LI('_create_qos_for_volume: not support'))
            return qos_info

        volume_name = self._construct_dsware_volume_name(volume)
        qos_info['qos_name'] = self._generate_qos_name(volume_name)

        LOG.info(_LI('dsware create qos: %s'), str(qos_info))
        if type(qos_info) != dict:
            raise exception.InvalidParameterValue('params type is not dict')
        qos_name = qos_info.get('qos_name')
        if not qos_name:
            raise exception.InvalidParameterValue('qos name is null')

        qos_info['result'] = self.dsware_client.create_qos(qos_info)
        LOG.info(_LI('create qos result: %s'), str(qos_info))

        return qos_info

    def _create_and_associate_qos_for_volume(self, volume):
        try:
            qos = self._create_qos_for_volume(volume)
        except Exception as e:
            LOG.error(traceback.format_exc())
            del_vol = self.delete_volume(volume)
            msg = (_("DSWARE Create Volume QoS failed! Exception:%(e)s, "
                     "Clear Volume Result:%(del_vol)s")
                   % {'e': e, 'del_vol': del_vol})
            raise exception.VolumeBackendAPIException(data=msg)

        if qos['result'] == 'not support':
            return

        if qos['result'] != 0:
            del_vol = self.delete_volume(volume)
            msg = (_("DSWARE Create Volume QoS failed! Result:%(qos)s, "
                     "Clear Volume Result:%(del_vol)s")
                   % {'qos': qos['result'], 'del_vol': del_vol})
            raise exception.VolumeBackendAPIException(data=msg)

        dsware_volume_name = self._construct_dsware_volume_name(volume)
        result = self.dsware_client.associate_qos_with_volume(
            qos['qos_name'], dsware_volume_name)
        if result != 0:
            del_vol = self.delete_volume(volume)
            del_qos = self.delete_qos(qos['qos_name'])
            msg = (_("DSWARE: Create Volume Associate Qos Failed! Result: "
                     "%(result)d, Clear Volume Result:%(dev_vol)d, "
                     "Clear Qos Result:%(del_qos)d") % {'result': result,
                                                        'del_vol': del_vol,
                                                        'del_qos': del_qos})
            raise exception.VolumeBackendAPIException(data=msg)

    def create_qos(self, qos):
        try:
            result = self.dsware_client.create_qos(qos)
        except Exception as e:
            LOG.error(_LE("create Qos error, details: %s"), e)
            raise e

        if result != 0:
            msg = _("DSWARE Create Qos failed! Result:%s") % result
            raise exception.VolumeBackendAPIException(data=msg)

    def delete_qos(self, qos_name):
        LOG.info(_LI('delete update qos: %s'), str(qos_name))
        try:
            result = self.dsware_client.delete_qos(qos_name)
        except Exception as e:
            LOG.error(_LE("Delete Qos error, details: %s"), e)
            raise e

        if result != 0:
            msg = _("DSWARE Delete Qos failed! Result:%s") % result
            raise exception.VolumeBackendAPIException(data=msg)

    def update_qos(self, qos):
        try:
            result = self.dsware_client.update_qos(qos)
        except Exception as e:
            LOG.error(_LE("Update Qos error, details: %s"), e)
            raise e

        if result != 0:
            msg = _("DSWARE Update Qos failed! Result:%s") % result
            raise exception.VolumeBackendAPIException(data=msg)

    def _build_volume_info(self, volume):
        replication_driver_data = json.loads(volume['replication_driver_data'])
        volume_info = {'name': replication_driver_data.get('vol_name'),
                       'id': replication_driver_data.get('lun_id')}
        return volume_info

    def associate_qos_with_volume(self, qos_id, volume):
        volume_info = self._build_volume_info(volume)

        LOG.info(_LI('dsware associate qos: %(qos_name)s, volume %(volume)s') %
                 {'qos_name': qos_id, 'volume': dict(volume_info)})

        vol_qos = self.query_volume_qos(volume_info)
        volume_name = self._construct_dsware_volume_name(volume_info)
        if vol_qos['result'] != 0:
            msg = _("DSWARE Associate Qos %(qos)s with volume %(volume)s "
                    "Query Qos failed! Result:%(result)s") % {
                      'qos': qos_id, 'volume': volume_name,
                      'result': vol_qos['result']}
            raise exception.VolumeBackendAPIException(data=msg)

        if vol_qos['para_num'] != 0:
            msg = _("DSWARE Associate Qos %(qos)s with volume %(volume)s "
                    "failed! Volume already associate Qos: %(volume_qos)s") % {
                      'qos': qos_id, 'volume': volume_name,
                      'volume_qos': vol_qos['qos_name']}
            raise exception.VolumeBackendAPIException(data=msg)

        new_qos = self._create_qos_for_volume(volume, qos_id)
        if new_qos['result'] == 'not support':
            msg = _("DSWARE Modify Qos %(qos)s with volume %(volume)s "
                    "Qos Config is Not Back-End!") % {'qos': qos_id,
                                                      'volume': volume_name}
            raise exception.VolumeBackendAPIException(data=msg)

        qos_name = new_qos['qos_name']

        try:
            result = self.dsware_client.associate_qos_with_volume(
                qos_name, volume_name)
        except Exception as e:
            LOG.error(_LE("Associate Qos %(qos)s with volume %(volume)s error,"
                          "details: %(exception)s"),
                      {'qos': qos_name, 'volume': volume_name, 'exception': e})
            raise e

        if result != 0:
            msg = _("DSWARE Associate Qos %(qos)s with volume %(volume)s "
                    "failed! Result:%(result)s") % {'qos': qos_name,
                                                    'volume': volume_name,
                                                    'result': result}
            raise exception.VolumeBackendAPIException(data=msg)

    def disassociate_qos_with_volume(self, qos_id, volume):
        vol_qos = self.query_volume_qos(volume)
        volume_name = self._construct_dsware_volume_name(volume)
        if vol_qos['result'] != 0:
            msg = _("DSWARE Disassociate Qos with volume %(volume)s Query Qos "
                    "failed! Result:%(vol_qos)s") % {'volume': volume_name,
                                                     'vol_qos': dict(vol_qos)}
            raise exception.VolumeBackendAPIException(data=msg)

        if vol_qos['para_num'] == 0:
            LOG.info(_LI('dsware disassociate volume%(volume)s qos:%(qos)s, '
                         'not associate qos') % {'volume': str(volume),
                                                 'qos': str(vol_qos)})
            return

        if not self._check_openstack_qos_name(vol_qos):
            msg = _("DSWARE Disassociate Qos %(qos)s with volume %(volume)s "
                    "failed! The QoS is User defined") % {
                      'qos': vol_qos['qos_name'], 'volume': volume_name}
            raise exception.VolumeBackendAPIException(data=msg)

        qos_name = vol_qos['qos_name']
        LOG.info(_LI('dsware disassociate qos:%(qos)s, volume%(volume)s'),
                 {'qos': str(qos_name), 'volume': str(volume)})

        try:
            result = self.dsware_client.disassociate_qos_with_volume(
                vol_qos['qos_name'], volume_name)
        except Exception as e:
            LOG.error(_LE("Disassociate Qos %(qos)s with volume %(volume)s "
                          "error, details: %(exception)s"),
                      {'qos': qos_name, 'volume': volume_name, 'exception': e})
            raise e

        if result != 0:
            msg = _("DSWARE Disassociate Qos %(qos)s with volume %(volume)s "
                    "failed! Result:%(result)s") % {'qos': qos_name,
                                                    'volume': volume_name,
                                                    'result': result}
            raise exception.VolumeBackendAPIException(data=msg)

        result = self.dsware_client.delete_qos(qos_name)
        if result != 0 and result != -157210:
            msg = _("DSWARE Disassociate Qos %(qos)s with volume %(volume)s,"
                    " Delete Qos failed! Result:%(result)s") % {
                      'qos': qos_name, 'volume': volume_name, 'result': result}
            raise exception.VolumeBackendAPIException(data=msg)

    def modify_qos_with_volume(self, qos_id, volume):
        volume_info = self._build_volume_info(volume)
        LOG.info(_LI('dsware modify volume: %(volume)s qos_id:%(qos)s,') %
                 {'volume': dict(volume_info), 'qos': qos_id})

        vol_qos = self.query_volume_qos(volume_info)
        if vol_qos['result'] != 0:
            msg = _("DSWARE Modify Qos with volume %(volume)s Query Qos "
                    "failed! Result:%(vol_qos)s") % {
                      'volume': dict(volume_info), 'vol_qos': dict(vol_qos)}
            raise exception.VolumeBackendAPIException(data=msg)

        if vol_qos['para_num'] == 0:
            msg = _("DSWARE Modify Qos %(vol_qos)s with volume %(volume)s "
                    "failed! Volume No associate Qos") % {
                      'vol_qos': dict(vol_qos), 'volume': dict(volume_info)}
            raise exception.VolumeBackendAPIException(data=msg)

        if not self._check_openstack_qos_name(vol_qos):
            msg = _("DSWARE Modify Qos %(vol_qos)s with volume %(volume)s "
                    "failed! User define Qos Can Not Modify!") % {
                      'vol_qos': dict(vol_qos), 'volume': dict(volume_info)}
            raise exception.VolumeBackendAPIException(data=msg)

        new_qos = self._create_qos_info(qos_id)
        if new_qos['result'] == 'not support':
            msg = _("DSWARE Modify Qos %(new_qos)s with volume %(volume)s Qos"
                    " Config is Not Back-End!") % {'new_qos': dict(new_qos),
                                                   'volume': dict(volume_info)}
            raise exception.VolumeBackendAPIException(data=msg)

        new_qos['qos_name'] = vol_qos['qos_name']

        self.update_qos(new_qos)

    def create_port(self, host):
        LOG.info(_LI("begin to create port in dsware: %s"), host['initiator'])
        port_name = host['initiator']
        try:
            result = self.dsware_client.create_port(port_name)
        except Exception as e:
            LOG.error(_LE("create port %s error, details: %s"), port_name, e)
            raise e

        if result != 0:
            msg = _("DSWARE Create Port failed! Result:%s") % result
            LOG.info(_LI(msg))
            raise exception.VolumeBackendAPIException(data=msg)
        else:
            return 0

    def delete_port(self, host):
        LOG.info(_LI("begin to delete port in dsware: %s"), host['initiator'])
        port_name = host['initiator']
        try:
            result = self.dsware_client.delete_port(port_name)
        except Exception as e:
            LOG.error(_LE("delete port %s error, details: %s"), port_name, e)
            raise e

        if result != 0:
            msg = _("DSWARE delete port failed! Result:%s") % result
            LOG.info(_LI(msg))

        return 0

    def add_port_to_host(self, host):
        port_name = host['initiator']
        host_name = host['host']
        LOG.info(_LI("begin to add port %s to host %s in dsware"),
                 port_name, host_name)

        try:
            result = self.dsware_client.add_port_to_host(host_name, port_name)
        except Exception as e:
            LOG.error(_LE("add port %s to host %s error, details: %s"),
                      port_name, host_name, e)
            raise e

        if result != 0:
            msg = _("DSWARE add port to host failed! Result:%s") % result
            LOG.info(_LI(msg))
            raise exception.VolumeBackendAPIException(data=msg)
        else:
            return 0

    def delete_port_from_host(self, host):
        port_name = host['initiator']
        host_name = host['host']
        LOG.info(_LI("begin to del port %s from host %s in dsware"),
                 port_name, host_name)

        try:
            result = \
                self.dsware_client.del_port_from_host(host_name, port_name)
        except Exception as e:
            LOG.error(_LE("del port %s from host %s error, details: %s"),
                      port_name, host_name, e)
            raise e

        if result != 0:
            msg = _("DSWARE del port from host failed! Result:%s") % result
            LOG.info(_LI(msg))

        return 0

    def add_volume_to_host(self, volume, host):
        volume_name = volume['name']
        host_name = host['host']
        LOG.info(_LI("begin to add volume %s to host %s in dsware"),
                 volume_name, host_name)

        try:
            result = self.dsware_client.add_lun_to_host(host_name, volume_name)
        except Exception as e:
            LOG.error(_LE("add lun %s to host %s error, details: %s"),
                      volume_name, host_name, e)
            raise e

        if result != 0:
            msg = _("DSWARE add lun mapping failed! Result:%s") % result
            LOG.info(_LI(msg))
            raise exception.VolumeBackendAPIException(data=msg)
        else:
            return 0

    def del_volume_from_host(self, volume, host):
        volume_name = volume['name']
        host_name = host['host']
        LOG.info(_LI("begin to del volume %s from host %s in dsware"),
                 volume_name, host_name)

        try:
            result = \
                self.dsware_client.del_lun_from_host(host_name, volume_name)
        except Exception as e:
            LOG.error(_LE("del lun %s from host %s error, details: %s"),
                      volume_name, host_name, e)
            raise e

        if result != 0:
            msg = _("DSWARE del lun from host failed! Result:%s") % result
            LOG.info(_LI(msg))

        return 0

    def create_host_group(self, host_group):
        LOG.info(_LI("begin to create host group %s in dsware"),
                 host_group['host_group_name'])
        host_group_name = host_group['host_group_name']
        try:
            result = \
                self.dsware_client.create_host_group(host_group_name)
        except Exception as e:
            LOG.error(_LE("create host group %s error, details: %s"),
                      host_group_name, e)
            raise e

        if result != 0:
            msg = _("DSWARE Create Host group failed! Result:%s") % result
            LOG.info(_LI(msg))
            raise exception.VolumeBackendAPIException(data=msg)
        else:
            return 0

    def delete_host_group(self, host_group):
        LOG.info(_LI("begin to delete host group %s in dsware"),
                 host_group['host_group_name'])
        host_group_name = host_group['host_group_name']
        try:
            result = \
                self.dsware_client.delete_host_group(host_group_name)
        except Exception as e:
            LOG.error(_LE("delete host group %s error, details: %s"),
                      host_group_name, e)
            raise e

        if result != 0:
            msg = _("DSWARE Delete Host Group failed! Result:%s") % result
            LOG.info(_LI(msg))

        return 0

    def add_host_to_hostgroup(self, host, hostgroup):
        LOG.info(_LI("begin to add host %s to hostgroup %s in dsware"),
                 host['host'], hostgroup['host_group_name'])
        host_name = host['host']
        group_name = hostgroup['host_group_name']
        try:
            result = \
                self.dsware_client.add_host_to_hostgroup(host_name, group_name)
        except Exception as e:
            LOG.error(_LE("add host %s to hostgroup %s error, details: %s"),
                      host_name, group_name, e)
            raise e

        if result != 0:
            msg = _("DSWARE add host to hostgroup failed! Result:%s") % result
            LOG.info(_LI(msg))
            raise exception.VolumeBackendAPIException(data=msg)
        else:
            return 0

    def delete_host_from_hostgroup(self, host, hostgroup):
        LOG.info(_LI("begin to delete host %s to hostgroup %s in dsware"),
                 host['host'], hostgroup['host_group_name'])
        host_name = host['host']
        group_name = hostgroup['host_group_name']
        try:
            result = \
                self.dsware_client.del_host_from_hostgroup(host_name, group_name)
        except Exception as e:
            LOG.error(_LE("delete host %s from hostgroup %s error, details: %s"),
                      host_name, group_name, e)
            raise e

        if result != 0:
            msg = _("delete host %s from hostgroup %s failed! Result:%s") % (host_name, group_name, result)
            LOG.info(_LI(msg))

        return 0

    def add_volume_to_hostgroup(self, volume, hostgroup):
        LOG.info(_LI("begin to add volume %s to hostgroup %s in dsware"),
                 volume['name'], hostgroup['host_group_name'])
        group_name = hostgroup['host_group_name']
        vol_name = volume['name']
        try:
            result = \
                self.dsware_client.add_volume_to_hostgroup(vol_name, group_name)
        except Exception as e:
            LOG.error(_LE("add volume to hostgroup error, details: %s"), e)
            raise e

        if result != 0:
            msg = _("add volume to hostgroup failed! Result:%s") \
                  % result
            LOG.info(_LI(msg))
            raise exception.VolumeBackendAPIException(data=msg)
        else:
            return 0

    def del_volume_from_hostgroup(self, volume, hostgroup):
        LOG.info(_LI("begin to delete volume %s from hostgroup %s in dsware"),
                 volume['name'], hostgroup['host_group_name'])
        group_name = hostgroup['host_group_name']
        vol_name = volume['name']
        try:
            result = \
                self.dsware_client.del_volume_from_hostgroup(vol_name, group_name)
        except Exception as e:
            LOG.error(_LE("delete volume %s from hostgroup %s error, details: %s"), vol_name, group_name, e)
            raise e

        if result != 0:
            msg = _("DSWARE delete volume from hostgroup failed! Result:%s") \
                  % result
            LOG.info(_LI(msg))

        return 0

    def construct_host_info(self, volume, connector, properties):
        LOG.info(_LI("begin to construct host info"))

        properties['description'] = 'huawei'
        properties['encrypted'] = None
        properties['qos_specs'] = None
        properties['access_mode'] = 'rw'
        properties['target_discovered'] = False
        properties['use_ultrapath_for_image_xfer'] = True
        properties['volume_id'] = volume['id']

        volume_name = self._get_dsware_volume_name(volume)
        volume_info = self.dsware_client.query_volume(volume_name)
        if volume_info:
            properties['lun_wwn'] = volume_info.get('wwn')
        else:
            msg = _("DSWARE get wwn failed! Result volume: ") % volume_name
            raise exception.VolumeBackendAPIException(data=msg)

        connector_ret = {'lun_id': volume_info.get('lun_id'),
                         'host_name': connector['host'],
                         'port_name': connector['initiator']}
        return {
            'driver_volume_type': 'iscsi',
            'host': connector['host'],
            'data': properties,
            'map_metadata': connector_ret
        }

    def _result_contains_lun_info(self, volume_info):
        result = False
        for line in volume_info:
            if "lunId" in line:
                result = True
            else:
                continue
        return result

    def get_portal_info(self, connector, properties, multi_path=True):
        target_num = 0
        port_name = connector['initiator']
        iqn_portal_list = \
            self.dsware_client.query_portal_info(port_name)
        if iqn_portal_list:
            if multi_path:
                portal_list = []
                portal_iqn_list = []
                for portal in iqn_portal_list:
                    portal_list.append(portal[:portal.find(',')])
                    portal_iqn_list.append(portal[portal.find(',') + 1:])
                    target_num += 1
                properties['target_iqn'] = portal_iqn_list
                properties['target_portal'] = portal_list
                properties['target_num'] = target_num
            else:
                portal = iqn_portal_list[0]
                LOG.info("get_portal_info in no multi_path, portal %s" % portal)
                portal_first = portal[:portal.find(',')]
                portal_iqn_first = portal[portal.find(',') + 1:]
                target_num += 1
                properties['target_iqn'] = portal_iqn_first
                properties['target_portal'] = portal_first
                properties['target_num'] = target_num
        else:
            self.delete_port(connector)
            msg = _("all iscsi switches of block clients is close, "
                    "please open at least one iscsi switch first.")
            LOG.error(_LI(msg))
            raise exception.VolumeBackendAPIException(data=msg)

    def get_target_lun_id(self, connector, volume):
        volume_name = self._get_dsware_volume_name(volume)
        if 'host' in connector:
            result = \
                self.dsware_client.query_host_lun_info(connector['host'])
        elif 'hosts' in connector:
            host_group_name = connector['host_group_name']
            result = \
                self.dsware_client.query_lun_from_hostgroup(host_group_name)
        else:
            msg = _("the host input error")
            LOG.error(_LI(msg))
            raise exception.VolumeBackendAPIException(data=msg)

        for line in result:
            # lun info mapping to host: '{lunId=1, lunName=vol1}'
            if "lunId" in line:
                line = line.split(',')
                index_1 = line[1].index('=')
                index_2 = line[1].index('}')
                target_lun_name = line[1][index_1 + 1: index_2]
                if target_lun_name == volume_name:
                    target_lun = line[0][7:]
                    return target_lun

    @staticmethod
    def create_consistencygroup(context, group):
        """Create consistency group."""
        model_update = {'status': 'available'}

        # Array will create CG at create_cgsnapshot time. Cinder will
        # maintain the CG and volumes relationship in the db.
        return model_update

    def delete_consistencygroup(self, context, group, volumes):
        model_update = {}
        volumes_model_update = []
        model_update.update({'status': group['status']})

        for volume in volumes:
            try:
                self.delete_volume(volume)
                volume.update({'status': 'deleted'})
                volumes_model_update.append(volume)
            except Exception:
                volume.update({'status': 'error_deleting'})
                volumes_model_update.append(volume)

        return model_update, volumes_model_update

    def update_consistencygroup(self, context, group,
                                add_volumes=None, remove_volumes=None):
        """Update consistency group."""
        model_update = {'status': 'available'}

        # Array will create CG at create_cgsnapshot time. Cinder will
        # maintain the CG and volumes relationship in the db.
        return model_update, None, None

    def create_host(self, host):
        LOG.info(_LI("begin to create host in dsware: %s"), host['host'])
        dsware_host_name = host['host']
        try:
            result = self.dsware_client.create_host(dsware_host_name)
        except Exception as e:
            LOG.error(_LE("create host error, details: %s"), e)
            raise e
        if result != 0:
            msg = _("DSWARE Create Host failed! Result:%s") % result
            raise exception.VolumeBackendAPIException(data=msg)
        else:
            return 0

    def delete_host(self, host):
        LOG.info(_LI("begin to delete host in dsware: %s"), host['host'])
        dsware_host_name = host['host']
        try:
            result = self.dsware_client.delete_host(dsware_host_name)
        except Exception as e:
            LOG.error(_LE("delete host error, details: %s"), e)
            raise e
        if result != 0:
            msg = _("DSWARE Delete Host failed! Result:%s") % result
            LOG.info(_LI(msg))

        return 0

    def backup_use_temp_snapshot(self):
        return CONF.backup_use_temp_snapshot

    def create_export(self, context, volume, connector=None):
        pass

    def ensure_export(self, context, volume):
        pass

    def remove_export(self, context, volume):
        pass

    def create_export_snapshot(self, context, snapshot, connector=None):
        pass

    def remove_export_snapshot(self, context, snapshot):
        pass


class DSWARELocalDriver(DSWAREDriver):
    """use Dsware local driver"""

    def __init__(self, *args, **kwargs):
        super(DSWARELocalDriver, self).__init__(*args, **kwargs)

    def _attach_volume_local(self, volume, manager_ip):
        LOG.info("begin to attach volume local")

        volume_name = self._get_dsware_volume_name(volume)

        if self._get_volume(volume_name) is False:
            msg = _("volume %s not exist") % volume_name
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        volume_attach_result = self.dsware_client.attach_volume(volume_name,
                                                                manager_ip)
        if volume_attach_result is None or int(
                volume_attach_result['result']) != 0:
            msg = _("attach volume failed")
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        volume_info = self.dsware_client.query_volume(volume_name)
        if volume_info is None or int(
                volume_info['result']) != 0 \
                or 'wwn' not in volume_info \
                or volume_info['wwn'] is None:
            msg = _("query volume for attach_volume failed, detach volume")
            LOG.error(msg)
            self._detach_volume_local(volume_name, manager_ip)
            raise exception.VolumeBackendAPIException(data=msg)

        LOG.info("attach_volume %s success", volume_name)
        volume_wwn = volume_info['wwn']
        attach_path = "/dev/disk/by-id/" + "wwn-0x%s" % volume_wwn
        properties = {'device_path': attach_path}
        wait_time = self.configuration.scan_device_timeout
        time.sleep(wait_time)
        LOG.info("wait %s second for scan device_path= %s", wait_time, attach_path)

        return {
            'driver_volume_type': 'local',
            'provider_volume_id': volume['id'],
            'data': properties
        }

    def _detach_volume_local(self, volume, manager_ip):
        LOG.info("begin to detach volume local")

        volume_name = self._get_dsware_volume_name(volume)
        if self.configuration.cross_node_detach:
            volume_detach_result = self.dsware_client.detach_volume_by_ip(volume_name,
                                                                          manager_ip)
        else:
            volume_detach_result = self.dsware_client.detach_volume(volume_name,
                                                                    manager_ip)
        if int(volume_detach_result['result']) != 0 \
                and int(volume_detach_result['result']) != 50151601:
            msg = _("detach volume failed")
            LOG.error(_LI(msg))
            raise exception.VolumeBackendAPIException(data=msg)

    def _get_manager_ip(self, context):
        LOG.info("get ip from host: %s", context['host'])
        if self.configuration.manager_ips.get(context['host']):
            return self.configuration.manager_ips.get(context['host'])
        else:
            msg = _("The required host: %(host)s and its manager ip are not "
                    "included in the configuration file."
                    ) % {"host": context['host']}
            raise exception.VolumeBackendAPIException(data=msg)

    def initialize_connection(self, volume, connector, initiator_data=None):
        """update volume host, ensure volume to image ok"""
        LOG.info("begin scsi initialize_connection")

        dsware_volume_name = self._get_dsware_volume_name(volume)
        # volume attach node manage ip
        manager_ip = self._get_manager_ip(connector)
        LOG.info("manage ip :%s, connector: %s." % (manager_ip, connector))
        return self._attach_volume_local(volume, manager_ip)

    def terminate_connection(self, volume, connector, force=False, **kwargs):
        LOG.info("begin scsi terminate_connection")
        manager_ip = self._get_manager_ip(connector)
        LOG.info("manage ip :%s.", manager_ip)
        self._detach_volume_local(volume, manager_ip)
        return

    def _attach_snapshot_backup(self, snapshot):
        LOG.info("begin to attach snapshot")
        dsw_manager_ip = self.dsware_client.get_manage_ip()
        snapshot_name = self._get_dsware_snap_name(snapshot)
        if self._get_snapshot(snapshot_name) is False:
            msg = _("snapshot %s not exist") % snapshot_name
            LOG.error(_LI(msg))
            raise exception.VolumeBackendAPIException(data=msg)
        snapshot_attach_result = self._dsware_attach_volume(snapshot_name,
                                                            dsw_manager_ip)
        if snapshot_attach_result is None or int(
                snapshot_attach_result['ret_code']) != 0:
            msg = _("attach snapshot failed")
            LOG.error(_LI(msg))
            raise exception.VolumeBackendAPIException(data=msg)
        snapshot_info = self.dsware_client.query_snap(snapshot_name)
        if snapshot_info is None or int(
                snapshot_info['result']) != 0 \
                or 'wwn' not in snapshot_info \
                or snapshot_info['wwn'] is None:
            msg = _("query snapshot for attach_snapshot failed, detach snapshot")
            LOG.error(_LI(msg))
            self._dsware_detach_volume(snapshot_name, dsw_manager_ip)
            raise exception.VolumeBackendAPIException(data=msg)
        snapshot_wwn = snapshot_info['wwn']
        attach_path = "/dev/disk/by-id/" + "wwn-0x%s" % snapshot_wwn
        properties = {'device_path': attach_path}
        LOG.info("device_path= %s", attach_path)
        return {
            'driver_volume_type': 'local',
            'provider_volume_id': snapshot['id'],
            'data': properties
        }

    def _detach_snapshot_backup(self, snapshot):
        LOG.info("begin to detach snapshot")
        dsw_manager_ip = self.dsware_client.get_manage_ip()
        snapshot_name = self._get_dsware_snap_name(snapshot)
        snapshot_detach_result = self._dsware_detach_volume(snapshot_name,
                                                            dsw_manager_ip)
        if int(snapshot_detach_result['ret_code']) != 0 \
                and int(snapshot_detach_result['ret_code']) != 50151601:
            msg = _("detach snapshot failed")
            LOG.error(_LI(msg))
            raise exception.VolumeBackendAPIException(data=msg)

    def initialize_connection_snapshot(self, snapshot, connector, **kwargs):
        LOG.info("begin to initialize_connection_snapshot")
        return self._attach_snapshot_backup(snapshot)

    def terminate_connection_snapshot(self, snapshot, connector, **kwargs):
        """Disallow connection from connector."""
        LOG.info("begin to terminate_connection_snapshot")
        self._detach_snapshot_backup(snapshot)
        return

    def _dsware_attach_volume(self, volume_name, dsw_manager_ip):
        cmd = ['vbs_cli', '-c', 'attachwithip', '-v', volume_name, '-i',
               dsw_manager_ip.replace('\n', ''), '-p', 0]
        out, err = self._execute(*cmd, run_as_root=True)
        analyse_result = self._analyse_output(out)
        LOG.info(_LI("vbs cmd is %s") % str(cmd))
        LOG.info(_LI("_dsware_attach_volume cmd out is %s") % analyse_result)
        return analyse_result

    def _dsware_detach_volume(self, volume_name, dsw_manager_ip):
        cmd = ['vbs_cli', '-c', 'detachwithip', '-v', volume_name, '-i',
               dsw_manager_ip.replace('\n', ''), '-p', 0]
        out, err = self._execute(*cmd, run_as_root=True)
        analyse_result = self._analyse_output(out)
        LOG.info(_LI("vbs cmd is %s") % str(cmd))
        LOG.info(_LI("_dsware_detach_volume cmd out:%s") % analyse_result)
        return analyse_result

    def copy_image_to_volume(self, context, volume, image_service, image_id):
        # Copy image to volume.
        # Step1: attach volume to host.
        LOG.debug("begin to copy image to volume")
        dsw_manager_ip = self.dsware_client.get_manage_ip()
        volume_name = self._get_dsware_volume_name(volume)
        volume_attach_result = self._dsware_attach_volume(volume_name,
                                                          dsw_manager_ip)
        volume_attach_path = ''
        if volume_attach_result is not None and int(
                volume_attach_result['ret_code']) == 0:
            volume_attach_path = volume_attach_result['dev_addr']
            LOG.debug("Volume attach path is %s.", volume_attach_path)
        if volume_attach_path == '':
            msg = _("Host attach volume failed!")
            raise exception.VolumeBackendAPIException(data=msg)
            # Step2: fetch the image from image_service and write it to the
            # volume.
        provider_location = None
        if volume.get('provider_location', None):
            provider_location = json.loads(volume.get('provider_location'))
        if provider_location and int(provider_location['offset']) != 0:
            volume_attach_path = self._dmsetup_create(volume_attach_path,
                                                      volume_name,
                                                      volume['size'])
        try:
            image_utils.fetch_to_raw(context,
                                     image_service,
                                     image_id,
                                     volume_attach_path,
                                     self.configuration.volume_dd_blocksize)

            if provider_location and int(provider_location['offset']) != 0:
                self._dmsetup_remove(volume_name)
        except Exception as e:
            raise e
        finally:
            # Step3: detach volume from host.
            volume_detach_result = self._dsware_detach_volume(volume_name,
                                                              dsw_manager_ip)
            if volume_detach_result is not None and int(
                    volume_detach_result['ret_code']) != 0:
                msg = (_("dsware detach volume from host failed: %s") %
                       volume_detach_result['ret_desc'])
                raise exception.VolumeBackendAPIException(data=msg)

    def copy_volume_to_image(self, context, volume, image_service, image_meta):
        # copy volume to image
        # step1 if volume was not attached,then attach it.

        dsw_manager_ip = self.dsware_client.get_manage_ip()
        volume_name = self._get_dsware_volume_name(volume)
        already_attached = True
        _attach_result = self._dsware_attach_volume(volume_name, dsw_manager_ip)
        if _attach_result:
            ret_code = _attach_result['ret_code']
            if int(ret_code) == 50151401:
                already_attached = False
                result = self._query_volume_attach(volume_name,
                                                   dsw_manager_ip)
                if not result or int(result['ret_code']) != 0:
                    msg = _("_query_volume_attach failed. result=%s") % result
                    raise exception.VolumeBackendAPIException(data=msg)

            elif int(ret_code) == 0:
                result = _attach_result
            else:
                msg = (_("attach volume to host failed "
                         "in copy volume to image, ret_code: %s") %
                       ret_code)
                raise exception.VolumeBackendAPIException(data=msg)

            volume_attach_path = result['dev_addr']

        else:
            msg = _("attach_volume failed.")
            LOG.error(_LE("attach_volume failed."))
            raise exception.VolumeBackendAPIException(data=msg)

        try:
            is_encrypted = None
            volume_cmkId = None
            if volume['volume_metadata'] is not None:
                for metadata in volume['volume_metadata']:
                    if metadata.key == '__system__encrypted':
                        is_encrypted = metadata.value
                    if metadata.key == '__system__cmkid':
                        volume_cmkId = metadata.value

            provider_location = None
            if volume.get('provider_location', None):
                provider_location = json.loads(volume.get('provider_location'))
            if provider_location and int(provider_location['offset']) != 0:
                volume_attach_path = self._dmsetup_create(volume_attach_path,
                                                          volume_name,
                                                          volume['size'])
            if (image_meta['disk_format'] == 'vhd') or (
                    is_encrypted is not None and volume_cmkId is not None):
                image_utils.upload_volume(context,
                                          image_service,
                                          image_meta,
                                          volume_attach_path,
                                          volume=volume)
            else:
                image_utils.upload_volume(context,
                                          image_service,
                                          image_meta,
                                          volume_attach_path)

            # update min_disk of image, otherwise min_disk of image
            # will be zero.
            image_metadata = {"min_disk": int(volume.get('size', 0))}
            image_id = image_meta.get('id')
            image_service.update(context, image_id, image_metadata,
                                 purge_props=False)

            if already_attached:
                if provider_location and int(provider_location['offset']) != 0:
                    self._dmsetup_remove(volume_name)
        except Exception as e:
            LOG.error(_LE("upload_volume error, details: %s"), e)
            raise e
        finally:
            if already_attached:
                self._dsware_detach_volume(volume_name, dsw_manager_ip)

    def _dsware_get_last_backup(self, backup_list):
        last_backup = None
        if backup_list:
            for back_tmp in backup_list:
                if (back_tmp['status'] != "available") and (
                        back_tmp['status'] != "restoring"):
                    continue
                if not last_backup:
                    last_backup = back_tmp
                if last_backup['created_at'] < back_tmp['created_at']:
                    last_backup = back_tmp

        return last_backup

    @staticmethod
    def _dsware_get_backup_metadata(backup):
        if 'service_metadata' in backup:
            metadata = backup.get('service_metadata')
            return json.loads(metadata)
        return {}

    def _original_backup_volume(self, context, backup, backup_service):
        volume = self.db.volume_get(context, backup['volume_id'])
        dsw_manager_ip = self.dsware_client.get_manage_ip()
        dsware_volume_name = self._get_dsware_volume_name(volume)
        LOG.debug('Creating a new backup for volume %s.',
                  dsware_volume_name)

        volume_attach_result = self._dsware_attach_volume(dsware_volume_name,
                                                          dsw_manager_ip)
        volume_attach_path = ''
        if volume_attach_result is not None and int(
                volume_attach_result['ret_code']) == 0:
            volume_attach_path = volume_attach_result['dev_addr']
            LOG.debug("volume_attach_path is %s", volume_attach_path)
        if volume_attach_path == '':
            msg = _("host attach volume failed")
            raise exception.VolumeBackendAPIException(data=msg)

        try:
            with utils.temporary_chown(volume_attach_path):
                with open(volume_attach_path) as volume_file:
                    backup_service.backup(backup, volume_file)
        except Exception as e:
            LOG.error(_LE("backup volume failed, exception:%s"), e)
            raise e
        finally:
            volume_detach_result = self._dsware_detach_volume(dsware_volume_name,
                                                              dsw_manager_ip)
            if volume_detach_result is not None and int(
                    volume_detach_result['ret_code']) != 0:
                msg = (_("DSware detach volume from host failed: %s") %
                       volume_detach_result['ret_desc'])
                raise exception.VolumeBackendAPIException(data=msg)

    def _get_last_volume_backup_info(self, source_volume_id, dsw_manager_ip,
                                     pool_id):
        volume_file = {}
        backup_list = self.db.backup_get_by_volume_id(context,
                                                      source_volume_id)
        last_backup = self._dsware_get_last_backup(backup_list)

        # Get backup type, do Full_backup or Incremental_backup.
        if not last_backup:
            volume_file['backup_type'] = 0
            volume_file['parent_id'] = None
            volume_file['parent_snapshot_url'] = None
        else:
            service_metadata = self._dsware_get_backup_metadata(last_backup)
            parent_snapshot_id = service_metadata.get('snap_id')
            LOG.debug("last_backup %s", last_backup['id'])
            volume_file['backup_type'] = 1
            volume_file['parent_id'] = last_backup['id']
            if not last_backup.get('service_metadata'):
                msg = _("backup service_metadata is none.")
                raise exception.InvalidVolumeMetadata(reason=msg)

            # Get last backup snapshot_url.
            if parent_snapshot_id:
                parent_snapshot_url = 'http://' + dsw_manager_ip + '/' + str(
                    pool_id) + '/' + 'snapshot-' + parent_snapshot_id
                volume_file['parent_snapshot_url'] = parent_snapshot_url
            else:
                msg = "parent_snapshot_id is none."
                raise exception.InvalidVolumeMetadata(reason=msg)
        return volume_file

    def backup_volume(self, context, backup, backup_service):
        """Create a new backup from an existing volume."""
        if not hasattr(backup_service, 'backup_driver_name'):
            self._original_backup_volume(context, backup, backup_service)
            return

        link_clone_vol = self.db.volume_get(context, backup['volume_id'])

        dsw_manager_ip = self.dsware_client.get_manage_ip()
        pool_id = self._get_poolid_from_host(link_clone_vol['host'])

        LOG.debug('Creating a new backup for volume %s.',
                  link_clone_vol['name'])
        snapshot_id = link_clone_vol['snapshot_id']
        source_snapshot = self.db.snapshot_get(context, snapshot_id)
        source_volume_id = source_snapshot['volume_id']
        volume_file = self._get_last_volume_backup_info(
            source_volume_id, dsw_manager_ip, pool_id)

        # Get current backup snapshot_url.
        if snapshot_id:
            snapshot_url = 'http://' + dsw_manager_ip + '/' + str(
                pool_id) + '/' + 'snapshot-' + snapshot_id
            volume_file['snapshot_url'] = snapshot_url
        else:
            msg = "snapshot_id is none."
            raise exception.InvalidVolumeMetadata(reason=msg)

        volume_file['snapshot_id'] = snapshot_id
        volume_file['source_volume_id'] = source_volume_id
        volume_file['storage_type'] = 1
        volume_file['bootable'] = False
        volume_file['image_id'] = None
        volume_file['parent_id'] = None
        volume_file['clone_volume_url'] = None

        try:
            backup_service.backup(backup, volume_file)
        except Exception as e:
            raise e
        finally:
            LOG.info(_LI('The volume_file is been send to ebackup server'))
            LOG.debug('cleanup for link_clone_volume %s.',
                      link_clone_vol['name'])

    def _original_restore_backup(self, context, backup,
                                 volume, backup_service):
        """
        :param context: object
        """
        dsw_manager_ip = self.dsware_client.get_manage_ip()
        dsware_volume_name = self._get_dsware_volume_name(volume)
        volume_attach_result = self._dsware_attach_volume(dsware_volume_name,
                                                          dsw_manager_ip)
        volume_attach_path = ''
        if volume_attach_result is not None and int(
                volume_attach_result['ret_code']) == 0:
            volume_attach_path = volume_attach_result['dev_addr']
            LOG.debug("volume_attach_path is %s", volume_attach_path)
        if volume_attach_path == '':
            msg = _("host attach volume failed")
            raise exception.VolumeBackendAPIException(data=msg)

        try:
            with utils.temporary_chown(volume_attach_path):
                with open(volume_attach_path, 'wb') as volume_file:
                    backup_service.restore(backup, volume['id'], volume_file)
        except Exception as e:
            LOG.error(_LE("restore volume failed, exception:%s"), e)
            raise e
        finally:
            volume_detach_result = self._dsware_detach_volume(dsware_volume_name,
                                                              dsw_manager_ip)
            if volume_detach_result is not None and int(
                    volume_detach_result['ret_code']) != 0:
                msg = (_("DSware detach volume from host failed: %s") %
                       volume_detach_result['ret_desc'])
                raise exception.VolumeBackendAPIException(data=msg)

    def restore_backup(self, context, backup, volume, backup_service):
        """Restore an existing backup to a new or existing volume."""
        dsware_volume_name = self._get_dsware_volume_name(volume)
        LOG.debug('Restoring backup %(backup)s to volume %(volume)s.',
                  {'backup': backup['id'],
                   'volume': dsware_volume_name})

        if not hasattr(backup_service, 'backup_driver_name'):
            self._original_restore_backup(context, backup,
                                          volume, backup_service)
            return

        dsw_manager_ip = self.dsware_client.get_manage_ip()

        pool_id_dst = self._get_poolid_from_host(volume['host'])

        src_volume = self.db.volume_get(context, backup['volume_id'])
        pool_id_src = self._get_poolid_from_host(src_volume['host'])

        volume_file = {}
        source_volume_id = backup['volume_id']
        backup_list = self.db.backup_get_by_volume_id(context,
                                                      source_volume_id)
        last_backup = self._dsware_get_last_backup(backup_list)

        if last_backup:
            service_metadata = self._dsware_get_backup_metadata(last_backup)
            latest_snapshot_id = service_metadata.get('snap_id')
        else:
            LOG.error(_LE(" Can't find last_backup, last_backup is: %s."),
                      last_backup)
            msg = "Can't find last_backup, last_backup is none."
            raise exception.InvalidVolumeMetadata(reason=msg)

        # Get last backup snapshot_url.
        if latest_snapshot_id:
            latest_snapshot_url = 'http://' + dsw_manager_ip + '/' + str(
                pool_id_src) + '/' + 'snapshot-' + latest_snapshot_id
            volume_file['latest_snapshot_url'] = latest_snapshot_url
        else:
            msg = "latest_snapshot_id is none."
            raise exception.InvalidVolumeMetadata(reason=msg)

        volume_file['storage_type'] = 1
        volume_file['restore_type'] = 0

        if source_volume_id == volume['id']:
            volume_file['restore_type'] = 1

        volume_url = 'http://' + dsw_manager_ip + '/' + str(
            pool_id_dst) + '/' + dsware_volume_name
        volume_file['volume_url'] = volume_url
        volume_file['clone_volume_url'] = None

        try:
            backup_service.restore(backup, volume['id'], volume_file)
        except Exception as e:
            raise e
        finally:
            LOG.info(_LI('The volume_file is been send to ebackup server'))

    def _check_volume_meta_data(self, volume_meta_data, properties):
        is_encrypted = None
        vol_cmkId = None
        if '__system__encrypted' in volume_meta_data:
            is_encrypted = volume_meta_data.get('__system__encrypted')
            vol_cmkId = volume_meta_data.get('__system__cmkid')
            self._check_volume_encrypt_metadata(properties, volume_meta_data)
        else:
            # add for tenant encryption
            if properties is not None:
                is_encrypted = properties.get('__system__encrypted', None)
                vol_cmkId = properties.get('__system__cmkid', None)
                if is_encrypted is not None and vol_cmkId is not None:
                    volume_meta_data['__system__encrypted'] = is_encrypted
                    volume_meta_data['__system__cmkid'] = vol_cmkId
        self._check_encrypted_metadata_valid(is_encrypted, vol_cmkId)

        if 'hw:passthrough' in volume_meta_data:
            hw_passthrough = volume_meta_data.get('hw:passthrough')
            if hw_passthrough is not None and str(
                    hw_passthrough).lower() == 'true':
                raise exception.InvalidParameterValue(
                    err=_('can not set hw:passthrough to '
                          'be true when create volume from image'))

    def clone_image(self, context, volume, image_location,
                    image_meta, image_service):
        """save the quick start sign to volume_admin_metadata
        :param image_location: object
        """
        LOG.info(_LI("[DSW-DRIVER] start clone_image [%s]"), image_meta['id'])
        properties = image_meta.get('properties', None)
        volume_meta_data = volume.get('metadata')
        if volume_meta_data:
            self._check_volume_meta_data(volume_meta_data, properties)
            # lazy loading case
            quick_start = properties.get('__quick_start', None)
            lazyLoading = properties.get('__lazyloading', None)
            if quick_start is not None and str(quick_start).lower() == 'true' and \
                    lazyLoading is not None and \
                    str(lazyLoading).lower() == 'true':
                if image_meta['disk_format'] in ['zvhd2', 'raw'] and \
                        'op_gated_lld' not in volume._context.roles and \
                        properties.get('__image_source_type') in ['uds', 'obs']:
                    updates = self.create_LLD_volume(volume, image_meta)
                    return updates, True

        # check quick start
        if properties is not None:
            quick_start = properties.get('__quick_start', None)
            if quick_start is not None and str(quick_start).lower() == 'true':
                LOG.info(_LI("[DSW-DRIVER] image has quick start property"))
                # update quick_start in admin metadata
                admin_context = cinder_context.get_admin_context()
                self.db.volume_admin_metadata_update(admin_context,
                                                     volume.get('id'),
                                                     {'__quick_start': 'True'},
                                                     False)
                min_disk = image_meta.get('min_disk')
                if min_disk:
                    model_update = dict(status='downloading')
                    self.db.volume_update(context,
                                          volume['id'], model_update)
                    updates = self._quick_create_volume(context,
                                                        volume,
                                                        image_service,
                                                        image_meta['id'],
                                                        min_disk)
                    return updates, True
                else:
                    msg = _('[DSW-DRIVER] image min_disk is none when '
                            'create from quick start image')
                    LOG.error(msg)
                    raise exception.VolumeDriverException(message=msg)
        return None, False


class DSWAREISCSIDriver(DSWAREDriver):
    """use Dsware iscsi driver"""

    def __init__(self, *args, **kwargs):
        super(DSWAREISCSIDriver, self).__init__(*args, **kwargs)

    def initialize_connection(self, volume, connector, initiator_data=None):
        """update volume host, ensure volume to image ok"""
        LOG.info("begin iscsi initialize_connection, connector: %s.", connector)
        multi_path = connector.get('multipath', False)

        properties = {}
        result = self.create_port(connector)
        if result != 0:
            LOG.error("create_port failed, result %s" % result)
            return result
        try:
            # the iscsi switch must be open
            self.get_portal_info(connector, properties, multi_path)
        except Exception as e:
            LOG.error("get_portal_info failed")
            raise e

        result = self.create_host(connector)
        if result != 0:
            msg = _("create_host failed, result %s" % result)
            LOG.error(_LI(msg))
            raise exception.VolumeBackendAPIException(data=msg)

        result = self.add_port_to_host(connector)
        if result != 0:
            msg = _("add_port_to_host failed, result %s" % result)
            LOG.error(_LI(msg))
            raise exception.VolumeBackendAPIException(data=msg)

        result = self.add_volume_to_host(volume, connector)
        if result != 0:
            msg = _("add_volume_to_host failed, result %s" % result)
            LOG.error(_LI(msg))
            raise exception.VolumeBackendAPIException(data=msg)

        target_lun = self.get_target_lun_id(connector, volume)
        if not target_lun:
            msg = _("get target_lun failed, volume %s" % volume)
            LOG.error(_LI(msg))
            raise exception.VolumeBackendAPIException(data=msg)

        properties['target_lun'] = target_lun

        host_info = self.construct_host_info(volume, connector, properties)
        LOG.info(_LI("[DSW-DRIVER] host_info %s"), host_info)
        return host_info

    def terminate_connection(self, volume, connector, force=False, **kwargs):
        LOG.info("begin iscsi terminate_connection")

        properties = {'iqn': connector['initiator']}
        host_info = {'driver_volume_type': 'iSCSI',
                     'data': properties}

        self.del_volume_from_host(volume, connector)

        volume_info = self.dsware_client.query_host_lun_info(connector['host'])
        result = self._result_contains_lun_info(volume_info)
        if result:
            LOG.error("del_volume_from_host, result %s" % result)
            return host_info
        else:
            self.delete_port_from_host(connector)
            self.delete_port(connector)
            self.delete_host(connector)
            LOG.error("delete port host")
            return host_info

    def initialize_connection_for_hostgroup(self, volume, connector, **kwargs):
        LOG.info("begin iscsi initialize_connection_for_hostgroup")

        properties = {}
        host_group = connector.get('host_group_name')

        result = self.create_host_group(connector)
        if result != 0:
            LOG.error("create_host_group, result %s" % result)
            return result
        hosts = connector.get('hosts')
        for host in hosts:
            result = self.create_port(host)
            if result != 0:
                LOG.error("create_port, result %s" % result)
                return result
            # the iscsi switch must be open
            self.get_portal_info(host, properties)
            result = self.create_host(host)
            if result != 0:
                LOG.error("create_host, result %s" % result)
                return result
            result = self.add_port_to_host(host)
            if result != 0:
                LOG.error("add_port_to_host, result %s" % result)
                return result

            result = self.add_host_to_hostgroup(host, connector)
            if result != 0:
                LOG.error("add_host_to_hostgroup, result %s" % result)
                return result

        result = self.add_volume_to_hostgroup(volume, connector)
        if result != 0:
            LOG.error("add_volume_to_hostgroup, result %s" % result)
            return result
        target_lun = self.get_target_lun_id(connector, volume)
        if not target_lun:
            self.terminate_connection_for_hostgroup(volume, connector)
            msg = _("get target_lun failed")
            LOG.error(_LI(msg))
            raise exception.VolumeBackendAPIException(data=msg)
        properties['target_lun'] = target_lun

        conn_info_list = []
        for host in hosts:
            conn_info = self.construct_host_info(volume, host, properties)
            conn_info['map_metadata']['host_group_name'] = host_group
            conn_info_list.append(conn_info)

        LOG.info(_LI("[DSW-DRIVER] iscsi host group info %s"), conn_info_list)
        LOG.debug("end initialize_connection_for_hostgroup")
        return conn_info_list

    def terminate_connection_for_hostgroup(self, volume, connector, **kwargs):
        hosts = connector['hosts']
        host_group = connector['host_group_name']
        if connector.get('hosts_num') == len(hosts):
            self.del_volume_from_hostgroup(volume, connector)
            volume_info = self.dsware_client.query_lun_from_hostgroup(host_group)
            result = self._result_contains_lun_info(volume_info)
            if result:
                return
            for host in hosts:
                self.delete_host_from_hostgroup(host, connector)
                volume_info_for_host = self.dsware_client.query_host_lun_info(host['host'])
                host_has_lun = self._result_contains_lun_info(volume_info_for_host)
                if host_has_lun:
                    continue
                else:
                    self.delete_port_from_host(host)
                    self.delete_port(host)
                    self.delete_host(host)

            self.delete_host_group(connector)
        else:
            for host in hosts:
                self.delete_host_from_hostgroup(host, connector)
                volume_info_for_host = self.dsware_client.query_host_lun_info(host['host'])
                host_has_lun = self._result_contains_lun_info(volume_info_for_host)
                if host_has_lun:
                    continue
                else:
                    self.delete_port_from_host(host)
                    self.delete_host(host)
