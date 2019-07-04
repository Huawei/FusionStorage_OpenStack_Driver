# Copyright (c) 2019 Huawei Technologies Co., Ltd.
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

from oslo_log import log as logging

import taskflow.engines
from taskflow.patterns import linear_flow
from taskflow import task
from taskflow.types import failure

from cinder.volume.drivers.fusionstorage import constants
from cinder.volume.drivers.fusionstorage import fs_utils


LOG = logging.getLogger(__name__)


class CheckLunInHostTask(task.Task):
    default_provides = 'is_lun_in_host'

    def __init__(self, client, *args, **kwargs):
        super(CheckLunInHostTask, self).__init__(*args, **kwargs)
        self.client = client

    def execute(self, host_name):
        is_lun_in_host = fs_utils.is_lun_in_host(self.client, host_name)
        if is_lun_in_host:
            LOG.info("The host %s is attached by other lun.", host_name)
        return is_lun_in_host


class CreateHostCheckTask(task.Task):
    def __init__(self, client, *args, **kwargs):
        super(CreateHostCheckTask, self).__init__(*args, **kwargs)
        self.client = client

    def execute(self, host_name):
        if not fs_utils.is_host_add_to_array(self.client, host_name):
            LOG.info("Create a new host: %s on the array", host_name)
            self.client.create_host(host_name)
        else:
            LOG.info("The host: %s is already on the array", host_name)


class DeleteHostWithCheck(task.Task):
    def __init__(self, client, *args, **kwargs):
        super(DeleteHostWithCheck, self).__init__(*args, **kwargs)
        self.client = client

    def execute(self, host_name, is_host_in_group):
        if not is_host_in_group and fs_utils.is_host_add_to_array(
                self.client, host_name):
            LOG.info("Delete host: %s from the array", host_name)
            self.client.delete_host(host_name)


class CreateHostGroupWithCheckTask(task.Task):
    def __init__(self, client, *args, **kwargs):
        super(CreateHostGroupWithCheckTask, self).__init__(*args, **kwargs)
        self.client = client

    def execute(self, host_group_name):
        if not fs_utils.is_hostgroup_add_to_array(
                self.client, host_group_name):
            LOG.info("Create a HostGroup: %s on the array", host_group_name)
            self.client.create_hostgroup(host_group_name)
        else:
            LOG.info("The HostGroup: %s is already on the array",
                     host_group_name)


class DeleteHostGroupWithCheck(task.Task):
    def __init__(self, client, *args, **kwargs):
        super(DeleteHostGroupWithCheck, self).__init__(*args, **kwargs)
        self.client = client

    def execute(self, host_group_name, is_host_in_group):
        if not is_host_in_group and fs_utils.is_host_group_empty(
                self.client, host_group_name):
            LOG.info("Delete HostGroup: %s from the array", host_group_name)
            self.client.delete_hostgroup(host_group_name)


class AddHostToHostGroupTask(task.Task):
    def __init__(self, client, *args, **kwargs):
        super(AddHostToHostGroupTask, self).__init__(*args, **kwargs)
        self.client = client

    def execute(self, host_name, host_group_name):
        if not fs_utils.is_host_in_host_group(self.client, host_name,
                                              host_group_name):
            LOG.info("Add host: %(host)s to HostGroup: %(HostGroup)s",
                     {"host": host_name, "HostGroup": host_group_name})
            self.client.add_host_to_hostgroup(host_group_name, host_name)
        else:
            LOG.info("The host: %(host)s is already in HostGroup: "
                     "%(HostGroup)s", {"host": host_name,
                                       "HostGroup": host_group_name})


class RemoveHostFromHostGroupWithCheck(task.Task):
    default_provides = 'is_host_in_group'

    def __init__(self, client, *args, **kwargs):
        super(RemoveHostFromHostGroupWithCheck, self).__init__(*args, **kwargs)
        self.client = client

    def execute(self, host_group_name, host_name, is_initiator_in_host):
        is_host_in_group = True
        if not is_initiator_in_host and fs_utils.is_host_in_host_group(
                self.client, host_name, host_group_name):
            LOG.info("Remove host: %(host)s from HostGroup: %(HostGroup)s",
                     {"host": host_name, "HostGroup": host_group_name})
            self.client.remove_host_from_hostgroup(host_group_name, host_name)
            is_host_in_group = False
        return is_host_in_group


class AddInitiatorWithCheckTask(task.Task):
    def __init__(self, client, *args, **kwargs):
        super(AddInitiatorWithCheckTask, self).__init__(*args, **kwargs)
        self.client = client

    def execute(self, initiator_name):
        if not fs_utils.is_initiator_add_to_array(self.client, initiator_name):
            LOG.info("Create a new initiator: %s on the array", initiator_name)
            self.client.add_initiator_to_array(initiator_name)
        else:
            LOG.info("The initiator: %s is already on the array",
                     initiator_name)


class RemoveInitiatorWithCheck(task.Task):
    def __init__(self, client, *args, **kwargs):
        super(RemoveInitiatorWithCheck, self).__init__(*args, **kwargs)
        self.client = client

    def execute(self, initiator_list, is_initiator_in_host):
        if not is_initiator_in_host:
            for initiator in initiator_list:
                host_list = self.client.get_host_associate_initiator(initiator)
                if not host_list:
                    LOG.info("Remove initiator: %s from the array", initiator)
                    self.client.remove_initiator_from_array(initiator)


class AssociateInitiatorToHostTask(task.Task):
    def __init__(self, client, *args, **kwargs):
        super(AssociateInitiatorToHostTask, self).__init__(*args, **kwargs)
        self.client = client

    def execute(self, initiator_name, host_name):
        if not fs_utils.is_initiator_associate_to_host(
                self.client, host_name, initiator_name):
            LOG.info("Associate initiator: %(initiator)s to host: %(host)s.",
                     {"initiator": initiator_name, "host": host_name})
            self.client.add_initiator_to_host(host_name, initiator_name)
        else:
            LOG.info("The initiator: %(initiator)s is already associate to "
                     "host: %(host)s.", {"initiator": initiator_name,
                                         "host": host_name})


class DeleteInitiatorFromHostWithCheck(task.Task):
    default_provides = ('is_initiator_in_host', 'initiator_list')

    def __init__(self, client, *args, **kwargs):
        super(DeleteInitiatorFromHostWithCheck, self).__init__(*args, **kwargs)
        self.client = client

    def execute(self, host_name, is_lun_in_host):
        is_initiator_in_host = True
        initiator_list = []
        if not is_lun_in_host:
            initiator_list = self.client.get_associate_initiator_by_host_name(
                host_name)
            for initiator in initiator_list:
                LOG.info("Dissociate initiator: %(init)s with host: %(host)s.",
                         {"init": initiator, "host": host_name})
                self.client.delete_initiator_from_host(host_name,
                                                       initiator)
            is_initiator_in_host = False
        return is_initiator_in_host, initiator_list


class MapLunToHostTask(task.Task):
    def __init__(self, client, *args, **kwargs):
        super(MapLunToHostTask, self).__init__(*args, **kwargs)
        self.client = client

    def execute(self, host_name, vol_name):
        LOG.info("Map lun: %(lun)s to host %(host)s.",
                 {"lun": vol_name, "host": host_name})
        self.client.map_volume_to_host(host_name, vol_name)

    def revert(self, result, host_name, vol_name, **kwargs):
        LOG.warning("Revert map lun to host task.")
        if isinstance(result, failure.Failure):
            return
        self.client.unmap_volume_from_host(host_name, vol_name)


class UnMapLunFromHostTask(task.Task):
    def __init__(self, client, *args, **kwargs):
        super(UnMapLunFromHostTask, self).__init__(*args, **kwargs)
        self.client = client

    def execute(self, host_name, vol_name):
        LOG.info("Unmap lun: %(lun)s with host %(host)s.",
                 {"lun": vol_name, "host": host_name})
        self.client.unmap_volume_from_host(host_name, vol_name)


class GetISCSIProperties(task.Task):
    default_provides = 'properties'

    def __init__(self, client, configuration, *args, **kwargs):
        super(GetISCSIProperties, self).__init__(*args, **kwargs)
        self.client = client
        self.configuration = configuration

    def execute(self, host_name, vol_name, multipath):
        LOG.info("Get ISCSI initialize connection properties.")
        target_ips = []
        target_iqns = []
        properties = {}

        target_lun = fs_utils.get_target_lun(self.client, host_name, vol_name)
        for ip in self.configuration.target_ips:
            target_ip, target_iqn = fs_utils.get_target_portal(
                self.client, ip)
            target_ips.append(target_ip)
            target_iqns.append(target_iqn)
            if not multipath:
                break

        if multipath:
            properties.update({
                "target_luns": [target_lun] * len(target_ips),
                "target_iqns": target_iqns,
                "target_portals": target_ips,
            })
        else:
            properties.update({
                "target_lun": target_lun,
                "target_iqn": target_iqns[0],
                "target_portal": target_ips[0],
            })

        return properties


def get_iscsi_required_params(vol_name, connector, client=None):
    if "host" in connector:
        host_name = connector['host']
        host_group_name = constants.HOST_GROUP_PREFIX + host_name
        initiator_name = connector['initiator']
        multipath = connector.get("multipath")
    else:
        host_list = client.get_host_by_volume(vol_name)
        if len(host_list) > 1:
            msg = ('Terminate_connection: multiple mapping of volume %(vol)s '
                   'found, no host specified, host_list: '
                   '%(host)s') % {'vol': vol_name, 'host': host_list}
            LOG.warning(msg)
            return None, None, None, None, None
        elif len(host_list) == 1:
            host_name = host_list[0]['hostName']
            host_group_name = constants.HOST_GROUP_PREFIX + host_name
            initiator_name = None
            multipath = None
        else:
            LOG.info("Terminate_connection: the volume %(vol)s does not map "
                     "to any host", {"vol": vol_name})
            return None, None, None, None, None

    LOG.info("Get iscsi required params. volume: %(vol)s, host: %(host)s,"
             " host_group: %(host_group)s, initiator: %(initiator)s, "
             "multipath: %(multipath)s",
             {"vol": vol_name, "host": host_name,
              "host_group": host_group_name, "initiator": initiator_name,
              "multipath": multipath})
    return vol_name, host_name, host_group_name, initiator_name, multipath


def initialize_iscsi_connection(client, vol_name, connector, configuration):
    (vol_name, host_name, host_group_name, initiator_name,
     multipath) = get_iscsi_required_params(vol_name, connector)

    store_spec = {'vol_name': vol_name,
                  'host_name': host_name,
                  'host_group_name': host_group_name,
                  'initiator_name': initiator_name,
                  'multipath': multipath}
    work_flow = linear_flow.Flow('initialize_iscsi_connection')

    if fs_utils.is_volume_associate_to_host(client, vol_name, host_name):
        LOG.info("Volume: %(vol)s has associated to the host: %(host)s",
                 {"vol": vol_name, "host": host_name})
    else:
        work_flow.add(
            CreateHostCheckTask(client),
            CreateHostGroupWithCheckTask(client),
            AddHostToHostGroupTask(client),
            AddInitiatorWithCheckTask(client),
            AssociateInitiatorToHostTask(client),
            MapLunToHostTask(client)
        )

    work_flow.add(
        GetISCSIProperties(client, configuration)
    )

    engine = taskflow.engines.load(work_flow, store=store_spec)
    engine.run()
    return engine.storage.fetch('properties')


def terminate_iscsi_connection(client, vol_name, connector):
    (vol_name, host_name, host_group_name,
     _, _) = get_iscsi_required_params(vol_name, connector, client)

    store_spec = {'vol_name': vol_name,
                  'host_name': host_name,
                  'host_group_name': host_group_name}
    work_flow = linear_flow.Flow('initialize_iscsi_connection')
    if host_name:
        if fs_utils.is_volume_associate_to_host(client, vol_name, host_name):
            work_flow.add(
                UnMapLunFromHostTask(client)
            )
        work_flow.add(
            CheckLunInHostTask(client),
            DeleteInitiatorFromHostWithCheck(client),
            RemoveInitiatorWithCheck(client),
            RemoveHostFromHostGroupWithCheck(client),
            DeleteHostWithCheck(client),
            DeleteHostGroupWithCheck(client)
        )

        engine = taskflow.engines.load(work_flow, store=store_spec)
        engine.run()
