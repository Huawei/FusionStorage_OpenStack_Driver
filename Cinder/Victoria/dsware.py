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

from multiprocessing import Lock
from oslo_config import cfg
from oslo_log import log as logging
from oslo_utils import units

from cinder import coordination
from cinder import exception
from cinder.i18n import _
from cinder import interface
from cinder.volume import driver
from cinder.volume.drivers.fusionstorage import constants
from cinder.volume.drivers.fusionstorage import fs_client
from cinder.volume.drivers.fusionstorage import fs_conf
from cinder.volume.drivers.fusionstorage import fs_flow
from cinder.volume.drivers.fusionstorage import fs_qos
from cinder.volume.drivers.fusionstorage import fs_utils
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
                    'semicolon(;) was used to split the storage pools, '
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
                     'IPV6 format')
]

CONF = cfg.CONF
CONF.register_opts(volume_opts)


@interface.volumedriver
class DSWAREBaseDriver(driver.VolumeDriver):
    VERSION = '2.2.RC4'
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
        url_str = self.configuration.san_address
        url_user = self.configuration.san_user
        url_password = self.configuration.san_password

        self.client = fs_client.RestCommon(
            fs_address=url_str, fs_user=url_user,
            fs_password=url_password)
        self.client.login()
        self.fs_qos = fs_qos.FusionStorageQoS(self.client)

    def check_for_setup_error(self):
        all_pools = self.client.query_pool_info()
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
                "driver_version": "2.2.RC4",
                "thin_provisioning_support": False,
                "pools": [],
                "vendor_name": "Huawei"
                }
        all_pools = self.client.query_pool_info()

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
        provisioned = float(pool_info['usedCapacity']) / units.Ki
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
            "QoS_support": True,
            'multiattach': True,
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

    @staticmethod
    def _raise_exception(msg):
        LOG.error(msg)
        raise exception.VolumeBackendAPIException(data=msg)

    def _get_pool_id(self, volume):
        pool_id_list = []
        pool_name = volume_utils.extract_host(volume.host, level='pool')
        all_pools = self.client.query_pool_info()
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

    def delete_volume(self, volume):
        vol_name = self._get_vol_name(volume)
        if self._check_volume_exist(volume):
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

    def create_volume_from_snapshot(self, volume, snapshot):
        vol_name = self._get_vol_name(volume)
        snapshot_name = self._get_snapshot_name(snapshot)
        vol_size = volume.size

        if not self._check_snapshot_exist(snapshot.volume, snapshot):
            msg = _("Snapshot: %(name)s does not exist!"
                    ) % {"name": snapshot_name}
            self._raise_exception(msg)
        elif self._check_volume_exist(volume):
            msg = _("Volume: %(vol_name)s already exists!"
                    ) % {'vol_name': vol_name}
            self._raise_exception(msg)
        else:
            vol_size *= units.Ki
            self.client.create_volume_from_snapshot(
                snapshot_name=snapshot_name, vol_name=vol_name,
                vol_size=vol_size)
            self._add_qos_to_volume(volume, vol_name)
            self._expand_volume_when_create(vol_name, vol_size)

    def create_cloned_volume(self, volume, src_volume):
        vol_name = self._get_vol_name(volume)
        src_vol_name = self._get_vol_name(src_volume)

        vol_size = volume.size
        vol_size *= units.Ki

        if not self._check_volume_exist(src_volume):
            msg = _("Volume: %(vol_name)s does not exist!"
                    ) % {"vol_name": src_vol_name}
            self._raise_exception(msg)
        else:
            self.client.create_volume_from_volume(
                vol_name=vol_name, vol_size=vol_size,
                src_vol_name=src_vol_name)
            self._add_qos_to_volume(volume, vol_name)
            self._expand_volume_when_create(vol_name, vol_size)

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

        provider_location = {"name": vol_name}
        return {'provider_location': json.dumps(provider_location)}

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
        if volume.host != host["host"]:
            msg = (_("Do not support retype between different host. Volume's "
                     "host: %(vol_host)s, host's host: %(host)s")
                   % {"vol_host": volume.host, "host": host['host']})
            LOG.error(msg)
            raise exception.InvalidInput(msg)

        old_opts = fs_utils.get_volume_specs(self.client, vol_name)
        new_opts = fs_utils.get_volume_type_params(new_type, self.client)
        if old_opts.get('qos') != new_opts.get('qos'):
            before_change["qos"] = old_opts.get("qos")
            after_change["qos"] = new_opts.get("qos")

        change_opts = {"old_opts": before_change,
                       "new_opts": after_change}
        return change_opts

    def retype(self, context, volume, new_type, diff, host):
        LOG.info("Retype volume: %(vol)s, new_type: %(new_type)s, "
                 "diff: %(diff)s, host: %(host)s",
                 {"vol": volume.id,
                  "new_type": new_type,
                  "diff": diff,
                  "host": host})

        vol_name = self._get_vol_name(volume)
        change_opts = self._check_need_changes_for_retype(
            volume, new_type, host, vol_name)
        self._change_lun(vol_name, change_opts.get("new_opts"),
                         change_opts.get("old_opts"))

        return True, None

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
        import time
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
        properties = fs_flow.initialize_iscsi_connection(
            self.client, vol_name, connector, self.configuration,
            self.manager_groups, self.lock)

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
