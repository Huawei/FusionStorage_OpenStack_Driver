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
LOG = logging.getLogger(__name__)


def is_volume_associate_to_host(client, vol_name, host_name):
    lun_host_list = client.get_host_by_volume(vol_name)
    for host in lun_host_list:
        if host.get('hostName') == host_name:
            return host.get("lunId")


def is_initiator_add_to_array(client, initiator_name):
    initiator_list = client.get_all_initiator_on_array()
    for initiator in initiator_list:
        if initiator.get('portName') == initiator_name:
            return initiator.get('portName')


def is_initiator_associate_to_host(client, host_name, initiator_name):
    initiator_list = client.get_associate_initiator_by_host_name(host_name)
    return initiator_name in initiator_list


def get_target_lun(client, host_name, vol_name):
    hostlun_list = client.get_host_lun(host_name)
    for hostlun in hostlun_list:
        if hostlun.get("lunName") == vol_name:
            return hostlun.get("lunId")


def get_target_portal(client, target_ip):
    tgt_portal = client.get_target_port(target_ip)
    for node_portal in tgt_portal:
        if node_portal.get("nodeMgrIp") == target_ip:
            port_list = node_portal.get("iscsiPortalList", [])
            for port in port_list:
                if port.get("iscsiStatus") == "active":
                    return port.get("iscsiPortal"), port.get('targetName')


def is_lun_in_host(client, host_name):
    hostlun_list = client.get_host_lun(host_name)
    return len(hostlun_list)


def is_host_add_to_array(client, host_name):
    all_hosts = client.get_all_host()
    for host in all_hosts:
        if host.get("hostName") == host_name:
            return host.get("hostName")


def is_hostgroup_add_to_array(client, host_group_name):
    all_host_groups = client.get_all_hostgroup()
    for host_group in all_host_groups:
        if host_group.get("hostGroupName") == host_group_name:
            return host_group.get("hostGroupName")


def is_host_group_empty(client, host_group_name):
    all_host = client.get_host_in_hostgroup(host_group_name)
    return not all_host


def is_host_in_host_group(client, host_name, host_group_name):
    all_host = client.get_host_in_hostgroup(host_group_name)
    return host_name in all_host
