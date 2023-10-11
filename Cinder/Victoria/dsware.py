# Copyright (c) 2018 Huawei Technologies Co., Ltd.
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

import json
import time
import uuid
from multiprocessing import Lock

import six
from oslo_config import cfg
from oslo_log import log as logging
from oslo_utils import excutils
from oslo_utils import units
from oslo_service import loopingcall

from cinder import coordination
from cinder import exception
from cinder.i18n import _
from cinder import interface
from cinder import objects
from cinder.volume import driver
from cinder.volume.drivers.fusionstorage import constants
from cinder.volume.drivers.fusionstorage import fs_client
from cinder.volume.drivers.fusionstorage import fs_conf
from cinder.volume.drivers.fusionstorage import fs_flow
from cinder.volume.drivers.fusionstorage import fs_qos
from cinder.volume.drivers.fusionstorage import fs_utils
from cinder.volume.drivers.fusionstorage import customization_driver
from cinder.volume.drivers.san import san
from cinder.volume import volume_utils

LOG = logging.getLogger(__name__)

volume_opts = [
    cfg.DictOpt('manager_ips',
                default={},
                help='This option is to support the FSA to mount across the '
                     'different nodes. The parameters takes the standard dict '
                     'config form, manager_ips = host1:ip1, host2:ip2...'),
    cfg.StrOpt('dsware_rest_url',
               default='',
               help='The address of FusionStorage array. For example, '
                    '"dsware_rest_url=xxx"'),
    cfg.StrOpt('dsware_storage_pools',
               default="",
               help='The list of pools on the FusionStorage array, the '
                    'semicolon(;) is used to split the storage pools, '
                    '"dsware_storage_pools = xxx1; xxx2; xxx3"'),
    cfg.ListOpt('target_ips',
                default=[],
                help='The ips of FSA node were used to find the target '
                     'initiator and target ips in ISCSI initialize connection.'
                     ' For example: "target_ips = ip1, ip2"'),
    cfg.IntOpt('scan_device_timeout',
               default=3,
               help='scan_device_timeout indicates the waiting time for '
                    'scanning device disks on the host. It only takes effect'
                    ' on SCSI. Default value is 3, the type is Int, the unit '
                    'is seconds. For example: "scan_device_timeout = 5"'),
    cfg.ListOpt('iscsi_manager_groups',
                default=[],
                help='The ip groups of FSA node were used to find the target '
                     'initiator and target ips in ISCSI in order to balance '
                     'business network. For example: '
                     '"iscsi_manager_groups = ip1;ip2;ip3, ip4;ip5;ip6"'),
    cfg.BoolOpt('use_ipv6',
                default=False,
                help='Whether to return target_portal and target_iqn in '
                     'IPV6 format'),
    cfg.BoolOpt('force_delete_volume',
                default=False,
                help='When deleting a LUN, if the LUN is in the mapping view,'
                     ' whether to delete it forcibly'),
    cfg.StrOpt('san_ip',
               default='',
               help='The ip address of FusionStorage array. For example, '
                    '"san_ip=xxx"'),
    cfg.StrOpt('san_port',
               default='',
               help='The port of FusionStorage array. For example, '
                    '"san_port=xxx"'),
    cfg.StrOpt('storage_pools',
               default="",
               help='The list of pools on the FusionStorage array, the '
                    'semicolon(;) is used to split the storage pools, '
                    '"storage_pools = xxx1; xxx2; xxx3"'),
    cfg.IntOpt('iscsi_link_count',
               default=4,
               help='Number of iSCSI links in an iSCSI network. '
                    'The default value is 4.'),
    cfg.BoolOpt('storage_ssl_two_way_auth',
               default=False,
               help='Whether to use mutual authentication.'),
    cfg.StrOpt('storage_ca_filepath',
               default='',
               help='CA certificate directory.'),
    cfg.StrOpt('storage_cert_filepath',
               default='',
               help='Client certificate directory.'),
    cfg.StrOpt('storage_key_filepath',
               default='',
               help='Client key directory.'),
    cfg.BoolOpt('full_clone',
                default=False,
                help='Whether use full clone.'),
    cfg.IntOpt('rest_timeout',
               default=constants.DEFAULT_TIMEOUT,
               help='timeout when call storage restful api.'),
]

CONF = cfg.CONF
CONF.register_opts(volume_opts)


@interface.volumedriver
class DSWAREBaseDriver(customization_driver.DriverForPlatform,
                       driver.VolumeDriver):
    VERSION = "2.6.2"
    CI_WIKI_NAME = 'Huawei_FusionStorage_CI'

    def __init__(self, *args, **kwargs):
        super(DSWAREBaseDriver, self).__init__(*args, **kwargs)

        if not self.configuration:
            msg = _('Configuration is not found.')
            LOG.error(msg)
            raise exception.InvalidInput(reason=msg)

        self.configuration.append_config_values(volume_opts)
        self.configuration.append_config_values(san.san_opts)
        self.conf = fs_conf.FusionStorageConf(self.configuration, self.host)
        self.client = None
        self.fs_qos = None
        self.manager_groups = self.configuration.iscsi_manager_groups
        self.lock = Lock()

    @staticmethod
    def get_driver_options():
        return volume_opts

    def do_setup(self, context):
        self.conf.update_config_value()
        self.conf.check_ssl_two_way_config_valid()
        url_str = self.configuration.san_address
        url_user = self.configuration.san_user
        url_password = self.configuration.san_password
        mutual_authentication = {
            "storage_ca_filepath": self.configuration.storage_ca_filepath,
            "storage_key_filepath": self.configuration.storage_key_filepath,
            "storage_cert_filepath": self.configuration.storage_cert_filepath,
            "storage_ssl_two_way_auth":
                self.configuration.storage_ssl_two_way_auth
        }

        extend_conf = {
            "mutual_authentication": mutual_authentication,
            "rest_timeout": self.configuration.rest_timeout
        }

        self.client = fs_client.RestCommon(fs_address=url_str,
                                           fs_user=url_user,
                                           fs_password=url_password,
                                           **extend_conf)
        self.client.login()
        self.fs_qos = fs_qos.FusionStorageQoS(self.client)

    def check_for_setup_error(self):
        all_pools = self.client.query_storage_pool_info()
        all_pools_name = [p['poolName'] for p in all_pools
                          if p.get('poolName')]

        for pool in self.configuration.pools_name:
            if pool not in all_pools_name:
                msg = _('Storage pool %(pool)s does not exist '
                        'in the FusionStorage.') % {'pool': pool}
                LOG.error(msg)
                raise exception.InvalidInput(reason=msg)

    def _update_pool_stats(self):
        backend_name = self.configuration.safe_get(
            'volume_backend_name') or self.__class__.__name__
        data = {"volume_backend_name": backend_name,
                "driver_version": self.VERSION,
                "pools": [],
                "vendor_name": "Huawei"
                }
        all_pools = self.client.query_storage_pool_info()

        for pool in all_pools:
            if pool['poolName'] in self.configuration.pools_name:
                single_pool_info = self._update_single_pool_info_status(pool)
                data['pools'].append(single_pool_info)
        return data

    def _get_capacity(self, pool_info):
        pool_capacity = {}

        total = float(pool_info['totalCapacity']) / units.Ki
        free = (float(pool_info['totalCapacity']) -
                float(pool_info['usedCapacity'])) / units.Ki
        provisioned = float(pool_info['allocatedCapacity']) / units.Ki
        pool_capacity['total_capacity_gb'] = total
        pool_capacity['free_capacity_gb'] = free
        pool_capacity['provisioned_capacity_gb'] = provisioned

        return pool_capacity

    def _update_single_pool_info_status(self, pool_info):
        status = {}
        capacity = self._get_capacity(pool_info=pool_info)
        status.update({
            "pool_name": pool_info['poolName'],
            "total_capacity_gb": capacity['total_capacity_gb'],
            "free_capacity_gb": capacity['free_capacity_gb'],
            "provisioned_capacity_gb": capacity['provisioned_capacity_gb'],
            "location_info": self.client.esn,
            "QoS_support": True,
            'multiattach': True,
            "thin_provisioning_support": True,
            'max_over_subscription_ratio':
                self.configuration.max_over_subscription_ratio,
            "reserved_percentage": self.configuration.safe_get('reserved_percentage'),
        })
        return status

    def get_volume_stats(self, refresh=False):
        self.client.keep_alive()
        stats = self._update_pool_stats()
        return stats

    def _check_volume_exist(self, volume):
        vol_name = self._get_vol_name(volume)
        result = self.client.query_volume_by_name(vol_name=vol_name)
        if result:
            return result

    def _check_volume_mapped(self, vol_name):
        host_list = self.client.get_host_by_volume(vol_name)
        if host_list and self.configuration.force_delete_volume:
            msg = ('Volume %s has been mapped to host.'
                   ' Now force to delete it') % vol_name
            LOG.warning(msg)
            for host in host_list:
                self.client.unmap_volume_from_host(host['hostName'], vol_name)
        elif host_list and not self.configuration.force_delete_volume:
            msg = 'Volume %s has been mapped to host' % vol_name
            self._raise_exception(msg)

    @staticmethod
    def _raise_exception(msg):
        LOG.error(msg)
        raise exception.VolumeBackendAPIException(data=msg)

    def _get_pool_id(self, volume):
        pool_name = volume_utils.extract_host(volume.host, level='pool')
        pool_id = self._get_pool_id_by_name(pool_name)
        return pool_id

    def _get_pool_id_by_name(self, pool_name):
        pool_id_list = []
        all_pools = self.client.query_storage_pool_info()
        for pool in all_pools:
            if pool_name == pool['poolName']:
                pool_id_list.append(pool['poolId'])
            if pool_name.isdigit() and int(pool_name) == int(pool['poolId']):
                pool_id_list.append(pool['poolId'])

        if not pool_id_list:
            msg = _('Storage pool %(pool)s does not exist on the array. '
                    'Please check.') % {"pool": pool_id_list}
            LOG.error(msg)
            raise exception.InvalidInput(reason=msg)

        # Prevent the name and id from being the same sign
        if len(pool_id_list) > 1:
            msg = _('Storage pool tag %(pool)s exists in multiple storage '
                    'pools %(pool_list). Please check.'
                    ) % {"pool": pool_name, "pool_list": pool_id_list}
            LOG.error(msg)
            raise exception.InvalidInput(reason=msg)

        return pool_id_list[0]

    @staticmethod
    def _get_vol_name(volume):
        vol_name = ""
        provider_location = volume.get("provider_location", None)

        if provider_location:
            try:
                provider_location = json.loads(provider_location)
                vol_name = (provider_location.get("name") or
                            provider_location.get('vol_name'))
            except Exception as err:
                LOG.warning("Get volume provider_location %(loc)s error. "
                            "Reason: %(err)s",
                            {"loc": provider_location, "err": err})

        if not vol_name:
            vol_name = volume.name
        return vol_name

    def _add_qos_to_volume(self, volume, vol_name):
        try:
            opts = fs_utils.get_volume_params(volume, self.client)
            if opts.get("qos"):
                self.fs_qos.add(opts["qos"], vol_name)
        except Exception:
            self.client.delete_volume(vol_name=vol_name)
            raise

    def create_volume(self, volume):
        pool_id = self._get_pool_id(volume)
        vol_name = volume.name
        vol_size = volume.size
        vol_size *= units.Ki
        self.client.create_volume(
            pool_id=pool_id, vol_name=vol_name, vol_size=vol_size)

        self._add_qos_to_volume(volume, vol_name)
        result = self.client.query_volume_by_name(vol_name=vol_name)
        return {"metadata": {'lun_wwn': result.get('wwn')}} if result else {}

    def delete_volume(self, volume):
        vol_name = self._get_vol_name(volume)
        if self._check_volume_exist(volume):
            self._check_volume_mapped(vol_name)
            self.fs_qos.remove(vol_name)
            self.client.delete_volume(vol_name=vol_name)

    def extend_volume(self, volume, new_size):
        vol_name = self._get_vol_name(volume)
        if not self._check_volume_exist(volume):
            msg = _("Volume: %(vol_name)s does not exist!"
                    ) % {"vol_name": vol_name}
            self._raise_exception(msg)
        else:
            new_size *= units.Ki
            self.client.expand_volume(vol_name, new_size)

    def _check_snapshot_exist(self, volume, snapshot):
        pool_id = self._get_pool_id(volume)
        snapshot_name = self._get_snapshot_name(snapshot)
        result = self.client.query_snapshot_by_name(
            pool_id=pool_id, snapshot_name=snapshot_name)
        return result if result else None

    def _get_snapshot_name(self, snapshot):
        snapshot_name = ""
        provider_location = snapshot.get("provider_location", None)
        if provider_location:
            try:
                provider_location = json.loads(provider_location)
                snapshot_name = (provider_location.get("name") or
                                 provider_location.get('snap_name'))
            except Exception as err:
                LOG.warning("Get snapshot provider_location %(loc)s error. "
                            "Reason: %(err)s",
                            {"loc": provider_location, "err": err})

        if not snapshot_name:
            snapshot_name = snapshot.name
        return snapshot_name

    def _expand_volume_when_create(self, vol_name, vol_size):
        try:
            vol_info = self.client.query_volume_by_name(vol_name)
            current_size = vol_info.get('volSize')
            if current_size < vol_size:
                self.client.expand_volume(vol_name, vol_size)
        except Exception:
            self.client.delete_volume(vol_name=vol_name)
            raise

    def _check_create_cloned_volume_finish(self, new_volume_name, start_time):
        LOG.debug('Loopcall: _check_create_cloned_volume_finish(), '
                  'volume-name %s',
                  new_volume_name)
        current_volume = self.client.query_volume_by_name(new_volume_name)

        if current_volume and 'status' in current_volume:
            status = int(current_volume['status'])
            LOG.debug('Wait clone volume %(volume_name)s, status:%(status)s.',
                      {"volume_name": new_volume_name,
                       "status": status})
            if status in {constants.REST_VOLUME_CREATING_STATUS,
                          constants.REST_VOLUME_DUPLICATE_VOLUME}:
                LOG.debug(_("Volume %s is cloning"), new_volume_name)
            elif status == constants.REST_VOLUME_CREATE_SUCCESS_STATUS:
                raise loopingcall.LoopingCallDone(retvalue=True)
            else:
                msg = _('Clone volume %(new_volume_name)s failed, '
                        'the status is:%(status)s.')
                LOG.error(msg, {'new_volume_name': new_volume_name,
                                'status': status})
                raise loopingcall.LoopingCallDone(retvalue=False)

            max_time_out = constants.CLONE_VOLUME_TIMEOUT
            current_time = time.time()
            if (current_time - start_time) > max_time_out:
                msg = _('Dsware clone volume time out. '
                        'Volume: %(new_volume_name)s, status: %(status)s')
                LOG.error(msg, {'new_volume_name': new_volume_name,
                                'status': current_volume['status']})
                raise loopingcall.LoopingCallDone(retvalue=False)
        else:
            LOG.warning(_('Can not find volume %s'), new_volume_name)
            msg = "DSWARE clone volume failed:volume can not find from dsware"
            LOG.error(msg)
            raise loopingcall.LoopingCallDone(retvalue=False)

    def _wait_for_create_cloned_volume_finish_timer(self, new_volume_name):
        timer = loopingcall.FixedIntervalLoopingCall(
            self._check_create_cloned_volume_finish,
            new_volume_name, time.time())
        LOG.debug('Calling _check_create_cloned_volume_finish: volume-name %s',
                  new_volume_name)
        ret = timer.start(interval=constants.CHECK_CLONED_INTERVAL).wait()
        timer.stop()
        return ret

    def create_volume_from_snapshot(self, volume, snapshot):
        vol_name = self._get_vol_name(volume)
        snapshot_name = self._get_snapshot_name(snapshot)
        vol_size = volume.size
        pool_id = self._get_pool_id(volume)

        if not self._check_snapshot_exist(snapshot.volume, snapshot):
            msg = _("Snapshot: %(name)s does not exist!"
                    ) % {"name": snapshot_name}
            self._raise_exception(msg)
        if self._check_volume_exist(volume):
            msg = _("Volume: %(vol_name)s already exists!"
                    ) % {'vol_name': vol_name}
            self._raise_exception(msg)

        vol_size *= units.Ki
        if not self.configuration.full_clone:
            self.client.create_volume_from_snapshot(
                snapshot_name=snapshot_name, vol_name=vol_name,
                vol_size=vol_size)
        else:
            self.client.create_volume(vol_name, vol_size, pool_id)
            self.client.create_full_volume_from_snapshot(vol_name,
                                                         snapshot_name)
            ret = self._wait_for_create_cloned_volume_finish_timer(vol_name)
            if not ret:
                msg = _('Create full volume %s from snap failed') % vol_name
                self._raise_exception(msg)
        self._add_qos_to_volume(volume, vol_name)
        self._expand_volume_when_create(vol_name, vol_size)
        result = self.client.query_volume_by_name(vol_name=vol_name)
        return ({"metadata": {'lun_wwn': result.get('wwn')}}
                if result else {})

    def _create_volume_from_volume_full_clone(self, vol_name, vol_size, pool_id,
                                              src_vol_name):
        tmp_snap_name = "temp" + src_vol_name + "clone" + vol_name

        self.client.create_snapshot(tmp_snap_name, src_vol_name)
        try:
            self.client.create_volume(vol_name, vol_size, pool_id)
        except Exception as err:
            self.client.delete_snapshot(tmp_snap_name)
            raise err

        try:
            self.client.create_full_volume_from_snapshot(
                vol_name, tmp_snap_name)
        except Exception as err:
            self.client.delete_snapshot(tmp_snap_name)
            self.client.delete_volume(vol_name)
            raise err

        ret = self._wait_for_create_cloned_volume_finish_timer(vol_name)
        if not ret:
            msg = _('Create full volume %s from snap failed') % vol_name
            self._raise_exception(msg)
        self.client.delete_snapshot(tmp_snap_name)

    def create_cloned_volume(self, volume, src_volume):
        vol_name = self._get_vol_name(volume)
        src_vol_name = self._get_vol_name(src_volume)

        vol_size = volume.size
        vol_size *= units.Ki

        if not self._check_volume_exist(src_volume):
            msg = _("Volume: %(vol_name)s does not exist!"
                    ) % {"vol_name": src_vol_name}
            self._raise_exception(msg)

        if not self.configuration.full_clone:
            self.client.create_volume_from_volume(
                vol_name=vol_name, vol_size=vol_size,
                src_vol_name=src_vol_name)
        else:
            pool_id = self._get_pool_id(volume)
            self._create_volume_from_volume_full_clone(
                vol_name=vol_name, vol_size=vol_size, pool_id=pool_id,
                src_vol_name=src_vol_name)

        self._add_qos_to_volume(volume, vol_name)
        self._expand_volume_when_create(vol_name, vol_size)
        result = self.client.query_volume_by_name(vol_name=vol_name)
        return ({"metadata": {'lun_wwn': result.get('wwn')}}
                if result else {})

    def create_snapshot(self, snapshot):
        snapshot_name = self._get_snapshot_name(snapshot)
        vol_name = self._get_vol_name(snapshot.volume)

        self.client.create_snapshot(
            snapshot_name=snapshot_name, vol_name=vol_name)

    def delete_snapshot(self, snapshot):
        snapshot_name = self._get_snapshot_name(snapshot)

        if self._check_snapshot_exist(snapshot.volume, snapshot):
            self.client.delete_snapshot(snapshot_name=snapshot_name)

    def _get_vol_info(self, pool_id, vol_name, vol_id):
        if vol_name:
            return self.client.query_volume_by_name(vol_name)

        elif vol_id:
            try:
                return self.client.query_volume_by_id(vol_id)
            except Exception:
                LOG.warning("Query volume info by id failed!")
                return self.client.get_volume_by_id(pool_id, vol_id)

    def _get_volume_info(self, pool_id, existing_ref):
        vol_name = existing_ref.get('source-name')
        vol_id = existing_ref.get('source-id')

        if not (vol_name or vol_id):
            msg = _('Must specify source-name or source-id.')
            raise exception.ManageExistingInvalidReference(
                existing_ref=existing_ref, reason=msg)

        vol_info = self._get_vol_info(pool_id, vol_name, vol_id)

        if not vol_info:
            msg = _("Can't find volume on the array, please check the "
                    "source-name or source-id.")
            raise exception.ManageExistingInvalidReference(
                existing_ref=existing_ref, reason=msg)
        return vol_info

    def _check_need_changes_for_manage(self, volume, vol_name):
        old_qos = {}
        new_qos = {}
        new_opts = fs_utils.get_volume_params(volume, self.client)
        old_opts = fs_utils.get_volume_specs(self.client, vol_name)

        # Not support from existence to absence or change
        if old_opts.get("qos"):
            if old_opts.get("qos") != new_opts.get("qos"):
                msg = (_("The current volume qos is: %(old_qos)s, the manage "
                         "volume qos is: %(new_qos)s")
                       % {"old_qos": old_opts.get("qos"),
                          "new_qos": new_opts.get("qos")})
                self._raise_exception(msg)
        elif new_opts.get("qos"):
            new_qos["qos"] = new_opts.get("qos")
            old_qos["qos"] = {}

        change_opts = {"old_opts": old_qos,
                       "new_opts": new_qos}

        return change_opts

    def _change_qos_remove(self, vol_name, new_opts, old_opts):
        if old_opts.get("qos") and not new_opts.get("qos"):
            self.fs_qos.remove(vol_name)

    def _change_qos_add(self, vol_name, new_opts, old_opts):
        if not old_opts.get("qos") and new_opts.get("qos"):
            self.fs_qos.add(new_opts["qos"], vol_name)

    def _change_qos_update(self, vol_name, new_opts, old_opts):
        if old_opts.get("qos") and new_opts.get("qos"):
            self.fs_qos.update(new_opts["qos"], vol_name)

    def _change_lun(self, vol_name, new_opts, old_opts):
        def _change_qos():
            self._change_qos_remove(vol_name, new_opts, old_opts)
            self._change_qos_add(vol_name, new_opts, old_opts)
            self._change_qos_update(vol_name, new_opts, old_opts)

        _change_qos()

    def manage_existing(self, volume, existing_ref):
        pool = self._get_pool_id(volume)
        vol_info = self._get_volume_info(pool, existing_ref)
        vol_pool_id = vol_info.get('poolId')
        vol_name = vol_info.get('volName')

        if pool != vol_pool_id:
            msg = (_("The specified LUN does not belong to the given "
                     "pool: %s.") % pool)
            raise exception.ManageExistingInvalidReference(
                existing_ref=existing_ref, reason=msg)

        change_opts = self._check_need_changes_for_manage(volume, vol_name)
        self._change_lun(vol_name, change_opts.get("new_opts"),
                         change_opts.get("old_opts"))
        meta_data = {'lun_wwn': vol_info.get('wwn')}
        provider_location = {"name": vol_name}
        return {"metadata": meta_data,
                'provider_location': json.dumps(provider_location)}

    def manage_existing_get_size(self, volume, existing_ref):
        pool = self._get_pool_id(volume)
        vol_info = self._get_volume_info(pool, existing_ref)
        remainder = float(vol_info.get("volSize")) % units.Ki

        if remainder != 0:
            msg = _("The volume size must be an integer multiple of 1 GB.")
            self._raise_exception(msg)

        size = float(vol_info.get("volSize")) / units.Ki
        return int(size)

    def unmanage(self, volume):
        return

    def _get_snapshot_info(self, volume, existing_ref):
        snapshot_name = existing_ref.get('source-name')
        if not snapshot_name:
            msg = _("Can't find volume on the array, please check the "
                    "source-name.")
            raise exception.ManageExistingInvalidReference(
                existing_ref=existing_ref, reason=msg)

        pool_id = self._get_pool_id(volume)
        snapshot_info = self.client.query_snapshot_by_name(
            pool_id, snapshot_name=snapshot_name)
        if not snapshot_info:
            msg = _("Can't find snapshot on the array.")
            raise exception.ManageExistingInvalidReference(
                existing_ref=existing_ref, reason=msg)

        return snapshot_info

    def _check_snapshot_match_volume(self, vol_name, snapshot_name):
        snapshot_info = self.client.query_snapshots_of_volume(
            vol_name, snapshot_name)
        return snapshot_info

    def manage_existing_snapshot(self, snapshot, existing_ref):
        volume = snapshot.volume
        snapshot_info = self._get_snapshot_info(volume, existing_ref)
        vol_name = self._get_vol_name(volume)
        if not self._check_snapshot_match_volume(
                vol_name, snapshot_info.get("snapName")):
            msg = (_("The specified snapshot does not belong to the given "
                     "volume: %s.") % vol_name)
            raise exception.ManageExistingInvalidReference(
                existing_ref=existing_ref, reason=msg)

        provider_location = {"name": snapshot_info.get('snapName')}
        return {'provider_location': json.dumps(provider_location)}

    def manage_existing_snapshot_get_size(self, snapshot, existing_ref):
        snapshot_info = self._get_snapshot_info(snapshot.volume, existing_ref)
        remainder = float(snapshot_info.get("snapSize")) % units.Ki

        if remainder != 0:
            msg = _("The snapshot size must be an integer multiple of 1 GB.")
            self._raise_exception(msg)
        size = float(snapshot_info.get("snapSize")) / units.Ki
        return int(size)

    def unmanage_snapshot(self, snapshot):
        return

    def _check_need_changes_for_retype(self, volume, new_type, host, vol_name):
        before_change = {}
        after_change = {}
        migrate = False
        if volume.host != host.get("host"):
            migrate = True
            msg = (_("retype support migration between different host. Volume's "
                     "host: %(vol_host)s, host's host: %(host)s")
                   % {"vol_host": volume.host, "host": host.get("host")})
            LOG.info(msg)

        old_opts = fs_utils.get_volume_specs(self.client, vol_name)
        new_opts = fs_utils.get_volume_type_params(new_type, self.client)
        if old_opts.get('qos') != new_opts.get('qos'):
            before_change["qos"] = old_opts.get("qos")
            after_change["qos"] = new_opts.get("qos")

        change_opts = {"old_opts": before_change,
                       "new_opts": after_change}
        return migrate, change_opts

    def retype(self, context, volume, new_type, diff, host):
        LOG.info("Retype volume: %(vol)s, new_type: %(new_type)s, "
                 "diff: %(diff)s, host: %(host)s",
                 {"vol": volume.id,
                  "new_type": new_type,
                  "diff": diff,
                  "host": host})

        vol_name = self._get_vol_name(volume)
        migrate, change_opts = self._check_need_changes_for_retype(
            volume, new_type, host, vol_name)
        if migrate:
            src_lun_id = self._check_volume_exist_on_array(volume)
            self._check_volume_snapshot_exist(volume)
            LOG.debug("Begin to migrate LUN(id: %(lun_id)s) with "
                      "change %(change_opts)s.",
                      {"lun_id": src_lun_id, "change_opts": change_opts})
            moved = self._migrate_volume(volume, host, src_lun_id)
            if not moved:
                LOG.warning("Storage-assisted migration failed during "
                            "retype.")
                return False, None
        self._change_lun(vol_name, change_opts.get("new_opts"),
                         change_opts.get("old_opts"))

        return True, None

    def migrate_volume(self, context, volume, host):
        """Migrate a volume within the same array."""
        LOG.info("Migrate Volume:%(volume)s, host:%(host)s",
                 {"volume": volume.id,
                  "host": host})
        src_lun_id = self._check_volume_exist_on_array(volume)
        self._check_volume_snapshot_exist(volume)

        moved = self._migrate_volume(volume, host, src_lun_id)
        return moved, {}

    def _create_dst_volume(self, volume, host):
        pool_name = host['capabilities']['pool_name']
        pool_id = self._get_pool_id_by_name(pool_name)
        vol_name = fs_utils.encode_name(six.text_type(uuid.uuid4()))
        vol_size = volume.size
        vol_size *= units.Ki
        self.client.create_volume(
            pool_id=pool_id, vol_name=vol_name, vol_size=vol_size)

        result = self.client.query_volume_by_name_v2(vol_name=vol_name)
        dst_lun_id = result.get('id')
        self._wait_volume_ready(vol_name)
        return vol_name, dst_lun_id

    def _migrate_volume(self, volume, host, src_lun_id):
        """create migration task and wait for task done"""
        if not self._check_migration_valid(host):
            return False

        vol_name, dst_lun_id = self._create_dst_volume(volume, host)

        try:
            self.client.create_lun_migration(src_lun_id, dst_lun_id)

            def _is_lun_migration_complete():
                return self._is_lun_migration_complete(src_lun_id, dst_lun_id)

            wait_interval = constants.MIGRATION_WAIT_INTERVAL
            fs_utils.wait_for_condition(_is_lun_migration_complete,
                                        wait_interval,
                                        constants.DEFAULT_WAIT_TIMEOUT)
        # Clean up if migration failed.
        except Exception as ex:
            raise exception.VolumeBackendAPIException(data=ex)
        finally:
            if self._is_lun_migration_exist(src_lun_id, dst_lun_id):
                self.client.delete_lun_migration(src_lun_id)
            self._delete_lun_with_check(vol_name)

        LOG.info("Migrate lun %s successfully.", src_lun_id)
        return True

    def _delete_lun_with_check(self, vol_name):
        if self.client.query_volume_by_name(vol_name):
            # migrate_dst_lun don't have qos, so don't
            # need to remove qos, Delete the LUN directly.
            self.client.delete_volume(vol_name)

    def _is_lun_migration_complete(self, src_lun_id, dst_lun_id):
        result = self.client.get_lun_migration_task_by_id(src_lun_id)
        found_migration_task = False
        if not result:
            return False

        if (str(src_lun_id) == result.get('parent_id') and
                str(dst_lun_id) == result.get('target_lun_id')):
            found_migration_task = True
            if constants.MIGRATION_COMPLETE == result.get('running_status'):
                return True
            if constants.MIGRATION_FAULT == result.get('running_status'):
                msg = _("Lun migration error. "
                        "the migration task running_status is abnormal")
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)

        if not found_migration_task:
            err_msg = _("lun migration error, Cannot find migration task.")
            LOG.error(err_msg)
            raise exception.VolumeBackendAPIException(data=err_msg)

        return False

    def _is_lun_migration_exist(self, src_lun_id, dst_lun_id):
        try:
            result = self.client.get_lun_migration_task_by_id(src_lun_id)
        except Exception:
            LOG.error("Get LUN migration error.")
            return False

        if (str(src_lun_id) == result.get('parent_id') and
                str(dst_lun_id) == result.get('target_lun_id')):
            return True
        return False

    def _wait_volume_ready(self, vol_name):
        wait_interval = constants.DEFAULT_WAIT_INTERVAL

        def _volume_ready():
            result = self.client.query_volume_by_name_v2(vol_name)
            if not result:
                return False

            if (result.get('health_status') == constants.STATUS_HEALTH
                    and result.get('running_status') == constants.STATUS_VOLUME_READY):
                return True
            return False

        fs_utils.wait_for_condition(_volume_ready,
                                    wait_interval,
                                    wait_interval * 10)

    def _check_migration_valid(self, host):
        if 'pool_name' not in host.get('capabilities', {}):
            return False

        target_device = host.get('capabilities', {}).get('location_info')

        # Source and destination should be on same array.
        if target_device != self.client.esn:
            LOG.error("lun migration error, "
                      "Source and destination should be on same array")
            return False

        pool_name = host.get('capabilities', {}).get('pool_name', '')
        if len(pool_name) == 0:
            LOG.error("lun migration error, pool_name not exists")
            return False

        return True

    def _check_volume_exist_on_array(self, volume):
        result = self._check_volume_exist(volume)
        if not result:
            msg = _("Volume %s does not exist on the array."
                    ) % volume.id
            self._raise_exception(msg)

        lun_wwn = result.get('wwn')
        if not lun_wwn:
            LOG.debug("No LUN WWN recorded for volume %s.", volume.id)

        lun_id = result.get('volId')
        if not lun_id:
            msg = _("Volume %s does not exist on the array."
                    ) % volume.id
            self._raise_exception(msg)

        return lun_id

    def _check_volume_snapshot_exist(self, volume):
        volume_name = self._get_vol_name(volume)
        if self.client.get_volume_snapshot(volume_name):
            msg = _("Volume %s which have snapshot cannot do lun migration"
                    ) % volume.id
            self._raise_exception(msg)

    def _rollback_snapshot(self, vol_name, snap_name):
        def _snapshot_rollback_finish():
            snapshot_info = self.client.get_snapshot_info_by_name(snap_name)
            if not snapshot_info:
                msg = (_("Failed to get rollback info with snapshot %s.")
                       % snap_name)
                self._raise_exception(msg)

            if snapshot_info.get('health_status') not in (
                    constants.SNAPSHOT_HEALTH_STATS_NORMAL,):
                msg = _("The snapshot %s is abnormal.") % snap_name
                self._raise_exception(msg)

            if (snapshot_info.get('rollback_progress') ==
                    constants.SNAPSHOT_ROLLBACK_PROGRESS_FINISH or
                    snapshot_info.get('rollback_endtime')):
                LOG.info("Snapshot %s rollback successful.", snap_name)
                return True
            return False

        if fs_utils.is_snapshot_rollback_available(self.client, snap_name):
            self.client.rollback_snapshot(vol_name, snap_name)

        try:
            fs_utils.wait_for_condition(
                _snapshot_rollback_finish, constants.WAIT_INTERVAL,
                constants.SNAPSHOT_ROLLBACK_TIMEOUT)
        except exception.VolumeBackendAPIException:
            self.client.cancel_rollback_snapshot(snap_name)
            raise

    def revert_to_snapshot(self, context, volume, snapshot):
        vol_name = self._get_vol_name(volume)
        snap_name = self._get_snapshot_name(snapshot)
        if not self._check_snapshot_exist(snapshot.volume, snapshot):
            msg = _("Snapshot: %(name)s does not exist!"
                    ) % {"name": snap_name}
            self._raise_exception(msg)

        if not self._check_volume_exist(volume):
            msg = _("Volume: %(vol_name)s does not exist!"
                    ) % {'vol_name': vol_name}
            self._raise_exception(msg)

        self._rollback_snapshot(vol_name, snap_name)

    def create_export(self, context, volume, connector):
        pass

    def ensure_export(self, context, volume):
        pass

    def remove_export(self, context, volume):
        pass

    def create_group(self, context, group):
        """Creates a group. Driver only need to return state"""
        if not volume_utils.is_group_a_cg_snapshot_type(group):
            LOG.info("Group's type is not a "
                     "consistent snapshot group enabled type")
            raise NotImplementedError()

        return self.create_consistencygroup(context, group)

    def create_consistencygroup(self, context, group):
        """Creates a group. Driver only need to return state"""
        LOG.info("Create group successfully")
        return {'status': 'available'}

    def update_group(self, context, group,
                     add_volumes=None, remove_volumes=None):
        """
        The volume manager will adds/removes the volume to/from the
        group in the database, if need add_volumes, Driver just need
         to check whether volume is in array or not
        """
        if not volume_utils.is_group_a_cg_snapshot_type(group):
            LOG.info("Group's type is not a "
                     "consistent snapshot group enabled type")
            raise NotImplementedError()

        return self.update_consistencygroup(
            context, group, add_volumes, remove_volumes)

    def update_consistencygroup(self, context, group,
                                add_volumes=None, remove_volumes=None):
        """
        The volume manager will adds/removes the volume to/from the
        group in the database, if need add_volumes, Driver just need
         to check whether volume is in array or not
        """
        if add_volumes is None:
            add_volumes = []
        for volume in add_volumes:
            try:
                self._check_volume_exist_on_array(volume)
            except Exception:
                with excutils.save_and_reraise_exception():
                    LOG.error("The add_volume %s not exist on array" % volume.id)

        model_update = {'status': 'available'}
        LOG.info("Update group successfully")
        return model_update, None, None

    def delete_group(self, context, group, volumes):
        """delete the group, Driver need to delete relation lun on array"""
        if not volume_utils.is_group_a_cg_snapshot_type(group):
            LOG.info("Group's type is not a "
                     "consistent snapshot group enabled type")
            raise NotImplementedError()

        return self.delete_consistencygroup(context, group, volumes)

    def delete_consistencygroup(self, context, group, volumes):
        """delete the group, Driver need to delete relation lun on array"""
        volumes_model_update = []
        model_update = {'status': 'deleted'}
        for volume in volumes:
            volume_model_update = {'id': volume.id}
            try:
                self.delete_volume(volume)
            except Exception:
                LOG.error('Delete volume %s failed.' % volume)
                volume_model_update.update({'status': 'error_deleting'})
            else:
                volume_model_update.update({'status': 'deleted'})

            LOG.info('Deleted volume %s successfully' % volume)
            volumes_model_update.append(volume_model_update)

        LOG.info("Delete group successfully")
        return model_update, volumes_model_update

    def create_group_from_src(self, context, group, volumes,
                              group_snapshot=None, snapshots=None,
                              source_group=None, source_vols=None):
        """
        create group from group or group-snapshot,
        Driver need to create volume from volume or snapshot
        """
        if not volume_utils.is_group_a_cg_snapshot_type(group):
            LOG.info("Group's type is not a "
                     "consistent snapshot group enabled type")
            raise NotImplementedError()

        return self.create_consistencygroup_from_src(context, group, volumes,
                                                     group_snapshot, snapshots,
                                                     source_group, source_vols)

    def create_consistencygroup_from_src(self, context, group, volumes,
                                         group_snapshot=None, snapshots=None,
                                         source_group=None, source_vols=None):
        """
        create group from group or group-snapshot,
        Driver need to create volume from volume or snapshot
        """
        model_update = self.create_consistencygroup(context, group)
        volumes_model_update = []
        delete_snapshots = False

        if not snapshots and source_vols:
            snapshots = []
            for src_volume in source_vols:
                volume_kwargs = {
                    'id': src_volume.id,
                    '_name_id': None,
                    'host': src_volume.host
                }
                snapshot_kwargs = {
                    'id': six.text_type(uuid.uuid4()),
                    'volume': objects.Volume(**volume_kwargs)
                }
                snapshots.append(objects.Snapshot(**snapshot_kwargs))

            self._create_group_snapshot(snapshots)
            delete_snapshots = True

        if snapshots:
            volumes_model_update = self._create_volume_from_group_snapshot(
                volumes, snapshots, delete_snapshots)

        if delete_snapshots:
            self._delete_group_snapshot(snapshots)

        LOG.info("Create group from src successfully")
        return model_update, volumes_model_update

    def _create_volume_from_group_snapshot(self, volumes, snapshots, delete_snapshots):
        volumes_model_update = []
        added_volumes = []
        for i, volume in enumerate(volumes):
            try:
                vol_model_update = self.create_volume_from_snapshot(
                    volume, snapshots[i])
                vol_model_update.update({'id': volume.id})
                volumes_model_update.append(vol_model_update)
                added_volumes.append(volume)
            except Exception:
                LOG.error("Create volume from snapshot error, Delete the newly created lun.")
                with excutils.save_and_reraise_exception():
                    self._delete_added_volume_snapshots(
                        added_volumes, snapshots, delete_snapshots)

        return volumes_model_update

    def _delete_added_volume_snapshots(
            self, added_volumes, snapshots, delete_snapshots):
        if delete_snapshots:
            self._delete_group_snapshot(snapshots)
        for add_volume in added_volumes:
            vol_name = self._get_vol_name(add_volume)
            self.fs_qos.remove(vol_name)
            self.client.delete_volume(vol_name=vol_name)
            LOG.info("delete storage newly added "
                     "volume success, volume is %s" % add_volume.id)

    def create_group_snapshot(self, context, group_snapshot, snapshots):
        """Create group snapshot."""
        if not volume_utils.is_group_a_cg_snapshot_type(group_snapshot):
            LOG.info("group_snapshot's type is not a "
                     "consistent snapshot group enabled type")
            raise NotImplementedError()

        return self.create_cgsnapshot(context, group_snapshot, snapshots)

    def create_cgsnapshot(self, context, group_snapshot, snapshots):
        """Create group snapshot."""
        LOG.info('Create group snapshot for group: %(group_snapshot)s',
                 {'group_snapshot': group_snapshot})

        try:
            snapshots_model_update = self._create_group_snapshot(snapshots)
        except Exception as err:
            msg = ("Create group snapshots failed. "
                   "Group snapshot id: %s. Reason is %s") % (group_snapshot.id, err)
            LOG.error(msg)
            raise exception.CinderException(msg)

        model_update = {'status': 'available'}
        return model_update, snapshots_model_update

    def _create_group_snapshot(self, snapshots):
        """Generate snapshots for all volumes in the group."""
        snapshots_model_update = []
        snapshot_group_list = []

        for snapshot in snapshots:
            snapshot_name = self._get_snapshot_name(snapshot)
            vol_name = self._get_vol_name(snapshot.volume)
            snapshot_group_list.append({
                'name': snapshot_name,
                'sub_type': '0',
                'volume_name': vol_name
            })
            snapshot_model_update = {
                'id': snapshot.id,
                'status': 'available',
            }
            snapshots_model_update.append(snapshot_model_update)
        try:
            self.client.create_consistent_snapshot_by_name(snapshot_group_list)
        except Exception:
            with excutils.save_and_reraise_exception():
                LOG.error("Create consistent snapshot failed, "
                          "snapshot_group_list is %s" % snapshot_group_list)

        return snapshots_model_update

    def delete_group_snapshot(self, context, group_snapshot, snapshots):
        """Delete group snapshot."""
        if not volume_utils.is_group_a_cg_snapshot_type(group_snapshot):
            LOG.info("group_snapshot's type is not a "
                     "consistent snapshot group enabled type")
            raise NotImplementedError()

        return self.delete_cgsnapshot(context, group_snapshot, snapshots)

    def delete_cgsnapshot(self, context, group_snapshot, snapshots):
        """Delete group snapshot."""
        LOG.info("Delete group_snapshot %s for "
                 "consistency group: %s" % (group_snapshot.id, group_snapshot))

        try:
            snapshots_model_update = self._delete_group_snapshot(snapshots)
        except Exception as err:
            msg = ("Delete cg snapshots failed. "
                   "group snapshot id: %s, reason is %s") % (group_snapshot.id, err)
            LOG.error(msg)
            raise exception.CinderException(msg)

        model_update = {'status': 'deleted'}
        return model_update, snapshots_model_update

    def _delete_group_snapshot(self, snapshots):
        """Delete all snapshots in snapshot group"""
        snapshots_model_update = []
        for snapshot in snapshots:
            snapshot_model_update = {
                'id': snapshot.id,
                'status': 'deleted'
            }
            snapshots_model_update.append(snapshot_model_update)
            snapshot_name = self._get_snapshot_name(snapshot)

            if not self._check_snapshot_exist(snapshot.volume, snapshot):
                LOG.info("snapshot %s not exist in array, "
                         "don't need to delete, try next one" % snapshot_name)
                continue
            self.client.delete_snapshot(snapshot_name=snapshot_name)
            LOG.info("Delete snapshot successfully,"
                     " the deleted snapshots is %s" % snapshot_name)

        return snapshots_model_update


class DSWAREDriver(DSWAREBaseDriver):
    def get_volume_stats(self, refresh=False):
        stats = DSWAREBaseDriver.get_volume_stats(self, refresh)
        stats['storage_protocol'] = 'SCSI'
        return stats

    def _get_manager_ip(self, context):
        if self.configuration.manager_ips.get(context['host']):
            return self.configuration.manager_ips.get(context['host'])
        else:
            msg = _("The required host: %(host)s and its manager ip are not "
                    "included in the configuration file."
                    ) % {"host": context['host']}
            self._raise_exception(msg)

    def _attach_volume(self, context, volume, properties, remote=False):
        vol_name = self._get_vol_name(volume)
        if not self._check_volume_exist(volume):
            msg = _("Volume: %(vol_name)s does not exist!"
                    ) % {"vol_name": vol_name}
            self._raise_exception(msg)
        manager_ip = self._get_manager_ip(properties)
        result = self.client.attach_volume(vol_name, manager_ip)
        attach_path = result[vol_name][0]['devName'].encode('unicode-escape')
        attach_info = dict()
        attach_info['device'] = dict()
        attach_info['device']['path'] = attach_path
        if attach_path == '':
            msg = _("Host attach volume failed!")
            self._raise_exception(msg)
        return attach_info, volume

    def _detach_volume(self, context, attach_info, volume, properties,
                       force=False, remote=False, ignore_errors=False):
        vol_name = self._get_vol_name(volume)
        if self._check_volume_exist(volume):
            manager_ip = self._get_manager_ip(properties)
            self.client.detach_volume(vol_name, manager_ip)

    def initialize_connection(self, volume, connector):
        vol_name = self._get_vol_name(volume)
        manager_ip = self._get_manager_ip(connector)
        if not self._check_volume_exist(volume):
            msg = _("Volume: %(vol_name)s does not exist!"
                    ) % {"vol_name": vol_name}
            self._raise_exception(msg)
        self.client.attach_volume(vol_name, manager_ip)
        volume_info = self.client.query_volume_by_name(vol_name=vol_name)
        vol_wwn = volume_info.get('wwn')
        by_id_path = "/dev/disk/by-id/" + "wwn-0x%s" % vol_wwn
        properties = {'device_path': by_id_path}

        LOG.info("Wait %(t)s second(s) for scanning the target device %(dev)s."
                 % {"t": self.configuration.scan_device_timeout,
                    "dev": by_id_path})
        time.sleep(self.configuration.scan_device_timeout)
        return {'driver_volume_type': 'local',
                'data': properties}

    def terminate_connection(self, volume, connector, **kwargs):
        attachments = volume.volume_attachment
        if volume.multiattach and len(attachments) > 1 and sum(
                1 for a in attachments if a.connector == connector) > 1:
            LOG.info("Volume is multi-attach and attached to the same host"
                     " multiple times")
            return

        if self._check_volume_exist(volume):
            manager_ip = self._get_manager_ip(connector)
            vol_name = self._get_vol_name(volume)
            self.client.detach_volume(vol_name, manager_ip)
        LOG.info("Terminate iscsi connection successful.")


class DSWAREISCSIDriver(DSWAREBaseDriver):
    def __init__(self, *args, **kwargs):
        super(DSWAREISCSIDriver, self).__init__(*args, **kwargs)
        self.support_iscsi_links_balance_by_pool = False

    def do_setup(self, context):
        super(DSWAREISCSIDriver, self).do_setup(context)
        if self.configuration.iscsi_manager_groups or self.configuration.target_ips:
            self.support_iscsi_links_balance_by_pool = False
        else:
            self.support_iscsi_links_balance_by_pool = \
                self.client.is_support_links_balance_by_pool()

    def check_for_setup_error(self):
        super(DSWAREISCSIDriver, self).check_for_setup_error()
        fs_utils.check_iscsi_group_valid(
            self.client, self.manager_groups, self.configuration.use_ipv6)

    def get_volume_stats(self, refresh=False):
        stats = DSWAREBaseDriver.get_volume_stats(self, refresh)
        stats['storage_protocol'] = 'iSCSI'
        return stats

    @coordination.synchronized('huawei-mapping-{connector[host]}')
    def initialize_connection(self, volume, connector):
        LOG.info("Start to initialize iscsi connection, volume: %(vol)s, "
                 "connector: %(con)s", {"vol": volume, "con": connector})
        if not self._check_volume_exist(volume):
            msg = _('The volume: %(vol)s is not on the '
                    'array') % {'vol': volume}
            LOG.error(msg)
            raise exception.InvalidInput(reason=msg)

        vol_name = self._get_vol_name(volume)
        pool_name = volume_utils.extract_host(volume.host, level='pool')
        iscsi_params = {
            'configuration': self.configuration,
            'manager_groups': self.manager_groups,
            'thread_lock': self.lock,
            'pool_name': pool_name,
            'support_iscsi_links_balance_by_pool': self.support_iscsi_links_balance_by_pool
        }
        properties = fs_flow.initialize_iscsi_connection(
            self.client, vol_name, connector, iscsi_params)

        LOG.info("Finish initialize iscsi connection, return: %s, the "
                 "remaining manager groups are %s",
                 properties, self.manager_groups)
        return {'driver_volume_type': 'iscsi', 'data': properties}

    def terminate_connection(self, volume, connector, **kwargs):
        host = connector['host'] if 'host' in connector else ""

        @coordination.synchronized('huawei-mapping-{host}')
        def _terminate_connection_locked(host):
            LOG.info("Start to terminate iscsi connection, volume: %(vol)s, "
                     "connector: %(con)s", {"vol": volume, "con": connector})
            attachments = volume.volume_attachment
            if volume.multiattach and len(attachments) > 1 and sum(
                    1 for a in attachments if a.connector == connector) > 1:
                LOG.info("Volume is multi-attach and attached to the same host"
                         " multiple times")
                return

            if not self._check_volume_exist(volume):
                LOG.info("Terminate_connection, volume %(vol)s is not exist "
                         "on the array ", {"vol": volume})
                return

            vol_name = self._get_vol_name(volume)
            fs_flow.terminate_iscsi_connection(
                self.client, vol_name, connector)

            LOG.info("Terminate iscsi connection successful.")
        return _terminate_connection_locked(host)
