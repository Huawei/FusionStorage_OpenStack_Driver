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

from oslo_log import log as logging
import requests
import six

from cinder import exception
from cinder.i18n import _
from cinder.volume.drivers.fusionstorage import constants

LOG = logging.getLogger(__name__)


class RestCommon(object):
    def __init__(self, fs_address, fs_user, fs_password):
        self.address = fs_address
        self.user = fs_user
        self.password = fs_password

        self.session = None
        self.token = None
        self.version = None

        self.init_http_head()

        LOG.warning("Suppressing requests library SSL Warnings")
        requests.packages.urllib3.disable_warnings(
            requests.packages.urllib3.exceptions.InsecureRequestWarning)
        requests.packages.urllib3.disable_warnings(
            requests.packages.urllib3.exceptions.InsecurePlatformWarning)

    def init_http_head(self):
        self.session = requests.Session()
        self.session.headers.update({
            "Content-Type": "application/json;charset=UTF-8",
        })
        self.session.verify = False

    def _construct_url(self, url, get_version, get_system_time):
        if get_system_time:
            return self.address + url
        elif get_version:
            return self.address + constants.BASIC_URI + url
        else:
            return self.address + constants.BASIC_URI + "v1.2" + url

    @staticmethod
    def _deal_call_result(result, filter_flag, json_flag, req_dict):
        if not filter_flag:
            LOG.info('''
            Request URL: %(url)s,
            Call Method: %(method)s,
            Request Data: %(data)s,
            Response Data: %(res)s,
            Result Data: %(res_json)s''', {'url': req_dict.get("url"),
                                           'method': req_dict.get("method"),
                                           'data': req_dict.get("data"),
                                           'res': result,
                                           'res_json': result.json()})

        return result.json() if json_flag else result

    def call(self, url, method, data=None,
             call_timeout=constants.DEFAULT_TIMEOUT, **input_kwargs):
        filter_flag = input_kwargs.get("filter_flag")
        json_flag = input_kwargs.get("json_flag", True)
        get_version = input_kwargs.get("get_version")
        get_system_time = input_kwargs.get("get_system_time")

        kwargs = {'timeout': call_timeout}
        if data is not None:
            kwargs['data'] = json.dumps(data)

        call_url = self._construct_url(url, get_version, get_system_time)
        func = getattr(self.session, method.lower())

        try:
            result = func(call_url, **kwargs)
        except Exception as err:
            LOG.error('Bad response from server: %(url)s. '
                      'Error: %(err)s'), {'url': call_url, 'err': err}
            return {"error": {
                "code": constants.CONNECT_ERROR,
                "description": "Connect to server error."}}

        try:
            result.raise_for_status()
        except requests.HTTPError as exc:
            return {"error": {"code": exc.response.status_code,
                              "description": six.text_type(exc)}}

        req_dict = {"url": call_url, "method": method, "data": data}
        return self._deal_call_result(result, filter_flag, json_flag, req_dict)

    def _assert_rest_result(self, result, err_str):
        if result.get('result') != 0:
            msg = (_('%(err)s\nresult: %(res)s.') % {'err': err_str,
                                                     'res': result})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

    def get_version(self):
        url = 'rest/version'
        self.session.headers.update({
            "Referer": self.address + constants.BASIC_URI
        })
        result = self.call(url=url, method='GET', get_version=True)
        self._assert_rest_result(result, _('Get version session error.'))
        if result.get("currentVersion"):
            self.version = result["currentVersion"]

    def login(self):
        self.get_version()
        url = '/sec/login'
        data = {"userName": self.user, "password": self.password}
        result = self.call(url, 'POST', data=data,
                           call_timeout=constants.LOGIN_SOCKET_TIMEOUT,
                           filter_flag=True, json_flag=False)
        self._assert_rest_result(result.json(), _('Login session error.'))
        self.token = result.headers['X-Auth-Token']

        self.session.headers.update({
            "x-auth-token": self.token
        })

    def logout(self):
        url = '/sec/logout'
        if self.address:
            result = self.call(url, 'POST')
            self._assert_rest_result(result, _('Logout session error.'))

    def keep_alive(self):
        url = '/sec/keepAlive'
        result = self.call(url, 'POST', filter_flag=True)

        if (result.get('result') == constants.ERROR_UNAUTHORIZED or
                result.get("errorCode") == constants.ERROR_USER_OFFLINE):
            try:
                self.login()
            except Exception:
                LOG.error('The FusionStorage may have been powered off. '
                          'Power on the FusionStorage and then log in.')
                raise
        else:
            self._assert_rest_result(result, _('Keep alive session error.'))

    def query_pool_info(self, pool_id=None):
        pool_id = str(pool_id)
        if pool_id != 'None':
            url = '/storagePool' + '?poolId=' + pool_id
        else:
            url = '/storagePool'
        result = self.call(url, 'GET', filter_flag=True)
        self._assert_rest_result(result, _("Query pool session error."))
        return result['storagePools']

    def _get_volume_num_by_pool(self, pool_id):
        pool_info = self.query_pool_info(pool_id)
        return pool_info[0].get('volumeNum', 0)

    def _query_volumes_by_batch(self, pool_id, page_num, page_size=1000):
        url = '/volume/list'
        params = {'poolId': pool_id,
                  'pageNum': page_num, 'pageSize': page_size}

        result = self.call(url, 'POST', params)
        if result.get('errorCode') in constants.VOLUME_NOT_EXIST:
            return None
        self._assert_rest_result(
            result, "Query all volume session error")
        return result.get('volumeList')

    def get_volume_by_id(self, pool_id, vol_id):
        vol_cnt = self._get_volume_num_by_pool(pool_id)
        page_num = constants.GET_VOLUME_PAGE_NUM
        page_size = constants.GET_VOLUME_PAGE_SIZE
        while vol_cnt > 0:
            vol_list = self._query_volumes_by_batch(pool_id, page_num,
                                                    page_size)
            for vol_info in vol_list:
                if int(vol_info.get('volId')) == int(vol_id):
                    return vol_info

            vol_cnt -= page_size
            page_num += 1
        return None

    def _query_snapshot_of_volume_batch(self, vol_name, snapshot_name,
                                        batch_num=1, batch_limit=1000):
        url = '/volume/snapshot/list'
        params = {"volName": vol_name, "batchLimit": batch_limit,
                  "batchNum": batch_num,
                  "filters": {"volumeName": snapshot_name}}
        result = self.call(url, "POST", params)
        self._assert_rest_result(
            result, 'Query snapshots of volume session error.')
        return result

    @staticmethod
    def _get_snapshot_from_result(batch_result, snapshot_key, snapshot_name):
        for res in batch_result.get('snapshotList', []):
            if res.get(snapshot_key) == snapshot_name:
                return res

    def query_snapshots_of_volume(self, vol_name, snapshot_name):
        batch_num = constants.GET_SNAPSHOT_PAGE_NUM
        batch_size = constants.GET_SNAPSHOT_PAGE_SIZE
        while True:
            batch_result = self._query_snapshot_of_volume_batch(
                vol_name, snapshot_name, batch_num, batch_size)
            snapshot_info = self._get_snapshot_from_result(
                batch_result, 'snapshotName', snapshot_name)
            if snapshot_info:
                return snapshot_info
            if batch_result.get('totalNum') < batch_size:
                break
            batch_num += 1
        return None

    def query_volume_by_name(self, vol_name):
        url = '/volume/queryByName?volName=' + vol_name
        result = self.call(url, 'GET')
        if result.get('errorCode') in constants.VOLUME_NOT_EXIST:
            return None
        self._assert_rest_result(
            result, _("Query volume by name session error"))
        return result.get('lunDetailInfo')

    def query_volume_by_id(self, vol_id):
        url = '/volume/queryById?volId=' + vol_id
        result = self.call(url, 'GET')
        if result.get('errorCode') in constants.VOLUME_NOT_EXIST:
            return None
        self._assert_rest_result(
            result, _("Query volume by ID session error"))
        return result.get('lunDetailInfo')

    def create_volume(self, vol_name, vol_size, pool_id):
        url = '/volume/create'
        params = {"volName": vol_name, "volSize": vol_size, "poolId": pool_id}
        result = self.call(url, "POST", params)
        self._assert_rest_result(result, _('Create volume session error.'))

    def delete_volume(self, vol_name):
        url = '/volume/delete'
        params = {"volNames": [vol_name]}
        result = self.call(url, "POST", params)
        self._assert_rest_result(result, _('Delete volume session error.'))

    def attach_volume(self, vol_name, manage_ip):
        url = '/volume/attach'
        params = {"volName": [vol_name], "ipList": [manage_ip]}
        result = self.call(url, "POST", params)
        self._assert_rest_result(result, _('Attach volume session error.'))

        if int(result[vol_name][0]['errorCode']) != 0:
            msg = _("Host attach volume failed!")
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        return result

    def detach_volume(self, vol_name, manage_ip):
        url = '/volume/detach/'
        params = {"volName": [vol_name], "ipList": [manage_ip]}
        result = self.call(url, "POST", params)
        self._assert_rest_result(result, _('Detach volume session error.'))

    def expand_volume(self, vol_name, new_vol_size):
        url = '/volume/expand'
        params = {"volName": vol_name, "newVolSize": new_vol_size}
        result = self.call(url, "POST", params)
        self._assert_rest_result(result, _('Expand volume session error.'))

    def _query_snapshot_by_name_batch(self, pool_id, snapshot_name,
                                      batch_num=1, batch_size=1000):
        url = '/snapshot/list'
        params = {"poolId": pool_id, "pageNum": batch_num,
                  "pageSize": batch_size,
                  "filters": {"volumeName": snapshot_name}}

        result = self.call(url, "POST", params)
        self._assert_rest_result(
            result, _('query snapshot list session error.'))
        return result

    def query_snapshot_by_name(self, pool_id, snapshot_name):
        batch_num = constants.GET_SNAPSHOT_PAGE_NUM
        batch_size = constants.GET_SNAPSHOT_PAGE_SIZE
        while True:
            batch_result = self._query_snapshot_by_name_batch(
                pool_id, snapshot_name, batch_num, batch_size)
            snapshot_info = self._get_snapshot_from_result(
                batch_result, 'snapName', snapshot_name)
            if snapshot_info:
                return snapshot_info
            if batch_result.get('totalNum') < batch_size:
                break
            batch_num += 1
        return None

    def create_snapshot(self, snapshot_name, vol_name):
        url = '/snapshot/create/'
        params = {"volName": vol_name, "snapshotName": snapshot_name}
        result = self.call(url, "POST", params)
        self._assert_rest_result(result, _('Create snapshot error.'))

    def delete_snapshot(self, snapshot_name):
        url = '/snapshot/delete/'
        params = {"snapshotName": snapshot_name}
        result = self.call(url, "POST", params)
        self._assert_rest_result(result, _('Delete snapshot session error.'))

    def create_volume_from_snapshot(self, snapshot_name, vol_name, vol_size):
        url = '/snapshot/volume/create/'
        params = {"src": snapshot_name, "volName": vol_name,
                  "volSize": vol_size}
        result = self.call(url, "POST", params)
        self._assert_rest_result(
            result, _('Create volume from snapshot session error.'))

    def create_volume_from_volume(self, vol_name, vol_size, src_vol_name):
        temp_snapshot_name = "temp" + src_vol_name + "clone" + vol_name

        self.create_snapshot(vol_name=src_vol_name,
                             snapshot_name=temp_snapshot_name)

        self.create_volume_from_snapshot(snapshot_name=temp_snapshot_name,
                                         vol_name=vol_name, vol_size=vol_size)

        self.delete_snapshot(snapshot_name=temp_snapshot_name)

    def create_host(self, host_name):
        url = '/host/create'
        params = {"hostName": host_name}
        result = self.call(url, "POST", params)
        self._assert_rest_result(result, _('Create host session error.'))

    def delete_host(self, host_name):
        url = '/host/delete'
        params = {"hostName": host_name}
        result = self.call(url, "POST", params)
        self._assert_rest_result(result, _('Delete host session error.'))

    def get_all_host(self):
        url = '/host/list'
        result = self.call(url, "GET")
        self._assert_rest_result(result, _('Get all host session error'))
        return result.get("hostList", [])

    def get_host_by_volume(self, vol_name):
        url = '/lun/host/list'
        params = {"lunName": vol_name}
        result = self.call(url, "POST", params)
        self._assert_rest_result(
            result, _("Get host by volume name session error"))
        return result.get("hostList", [])

    def map_volume_to_host(self, host_name, vol_name):
        url = '/host/lun/add'
        params = {"hostName": host_name, "lunNames": [vol_name]}
        result = self.call(url, "POST", params)
        self._assert_rest_result(
            result, _("Map volumes to host session error"))

    def unmap_volume_from_host(self, host_name, vol_name):
        url = '/host/lun/delete'
        params = {"hostName": host_name, "lunNames": [vol_name]}
        result = self.call(url, "POST", params)
        self._assert_rest_result(
            result, _("Unmap volumes from host session error"))

    def get_host_lun(self, host_name):
        url = '/host/lun/list'
        params = {"hostName": host_name}
        result = self.call(url, "POST", params)
        self._assert_rest_result(
            result, "Get host mapped lun info session error")
        return result.get("hostLunList", [])

    def get_associate_initiator_by_host_name(self, host_name):
        url = '/port/host/list'
        params = {"hostName": host_name}
        result = self.call(url, "POST", params)
        self._assert_rest_result(
            result, "Get associate initiator by host name session error")
        return result.get("portList", [])

    def create_hostgroup(self, host_group_name):
        url = '/hostGroup/add'
        params = {"hostGroupName": host_group_name}
        result = self.call(url, "POST", params)
        self._assert_rest_result(
            result, _("Create HostGroup session error"))

    def delete_hostgroup(self, host_group_name):
        url = '/hostGroup/delete'
        params = {"hostGroupName": host_group_name}
        result = self.call(url, "POST", params)
        self._assert_rest_result(
            result, _("Delete HostGroup session error"))

    def get_all_hostgroup(self):
        url = '/hostGroup/list'
        result = self.call(url, "GET")
        self._assert_rest_result(result, _("Get HostGroup session error"))
        return result.get("groupList", [])

    def add_host_to_hostgroup(self, host_group_name, host_name):
        url = '/hostGroup/host/add'
        params = {"hostGroupName": host_group_name, "hostList": [host_name]}
        result = self.call(url, "POST", params)
        self._assert_rest_result(
            result, _("Add host to HostGroup session error"))

    def remove_host_from_hostgroup(self, host_group_name, host_name):
        url = '/hostGroup/host/delete'
        params = {"hostGroupName": host_group_name, "hostList": [host_name]}
        result = self.call(url, "POST", params)
        self._assert_rest_result(
            result, _("Delete host from HostGroup session error"))

    def get_host_in_hostgroup(self, host_group_name):
        url = '/hostGroup/host/list'
        params = {"hostGroupName": host_group_name}
        result = self.call(url, "POST", params)
        self._assert_rest_result(
            result, _("Get host in HostGroup session error"))
        return result.get("hostList", [])

    def get_all_initiator_on_array(self):
        url = '/port/list'
        params = {}
        result = self.call(url, "POST", params)
        self._assert_rest_result(
            result, _("Get all initiator on array session error"))
        return result.get("portList", [])

    def add_initiator_to_array(self, initiator_name):
        url = 'iscsi/createPort'
        params = {"portName": initiator_name}
        result = self.call(url, "POST", params, get_version=True)
        self._assert_rest_result(
            result, _("Add initiator to array session error"))

    def remove_initiator_from_array(self, initiator_name):
        url = 'iscsi/deletePort'
        params = {"portName": initiator_name}
        result = self.call(url, "POST", params, get_version=True)
        self._assert_rest_result(
            result, _("Remove initiator from array session error"))

    def add_initiator_to_host(self, host_name, initiator):
        url = '/host/port/add'
        params = {"hostName": host_name, "portNames": [initiator]}
        result = self.call(url, "POST", params)
        self._assert_rest_result(
            result, _("Add initiator to host session error"))

    def delete_initiator_from_host(self, host_name, initiator):
        url = '/host/port/delete'
        params = {"hostName": host_name, "portNames": [initiator]}
        result = self.call(url, "POST", params)
        self._assert_rest_result(
            result, _("Delete initiator from host session error"))

    def get_host_associate_initiator(self, initiator):
        url = '/host/port/list'
        params = {"portName": [initiator]}
        result = self.call(url, "POST", params)
        self._assert_rest_result(
            result, _("Get host by initiator session error"))
        return result['portHostMap'].get(initiator, [])

    def get_target_port(self, target_ip):
        url = "/iscsi/port/list"
        params = {"nodeMgrIps": [target_ip]}
        result = self.call(url, "POST", params)
        self._assert_rest_result(
            result, _("Get iscsi port info session error"))
        return result.get("nodeResultList", [])

    def create_qos(self, qos_name, qos_params):
        url = "/qos/create"
        params = {"qosName": qos_name, "qosSpecInfo": qos_params}
        result = self.call(url, "POST", params)
        self._assert_rest_result(
            result, _("Create QoS session error"))

    def delete_qos(self, qos_name):
        url = "/qos/delete"
        params = {"qosNames": [qos_name]}
        result = self.call(url, "POST", params)
        self._assert_rest_result(
            result, _("Delete QoS session error"))

    def modify_qos(self, qos_name, qos_params):
        url = "/qos/modify"
        params = {"qosName": qos_name, "qosSpecInfo": qos_params}
        result = self.call(url, "POST", params)
        self._assert_rest_result(
            result, _("Modify QoS session error"))

    def associate_qos_with_volume(self, vol_name, qos_name):
        url = "/qos/volume/associate"
        params = {"keyNames": [vol_name], "qosName": qos_name}
        result = self.call(url, "POST", params)
        self._assert_rest_result(
            result, _("Associate QoS with volume session error"))

    def disassociate_qos_with_volume(self, vol_name, qos_name):
        url = "/qos/volume/disassociate"
        params = {"keyNames": [vol_name], "qosName": qos_name}
        result = self.call(url, "POST", params)
        self._assert_rest_result(
            result, _("Disassociate QoS with volume session error"))

    def get_qos_by_vol_name(self, vol_name):
        url = "/volume/qos?volName=%s" % vol_name
        result = self.call(url, "GET")
        self._assert_rest_result(
            result, _("Get QoS by volume name session error"))

        return result

    def get_qos_volume_info(self, pool_id, qos_name,
                            batch_num=1, batch_size=5):
        url = "/qos/volume/list?type=associated"
        params = {"pageNum": batch_num,
                  "pageSize": batch_size,
                  "queryType": "volume",
                  "qosName": qos_name,
                  "poolId": pool_id}

        result = self.call(url, "POST", params)
        self._assert_rest_result(
            result, _("Get QoS info session error"))
        return result.get("volumes", [])

    def get_fsm_version(self):
        url = "/version"
        result = self.call(url, "GET")
        self._assert_rest_result(
            result, _("Get FSM version session error."))
        return result.get("version")

    def get_system_time_zone(self):
        url = "/time/querytimezone"
        result = self.call(url, "GET")
        self._assert_rest_result(
            result, _("Get system time zone session error."))

        return result.get("timeZone")

    def get_time_config(self):
        url = "/api/v2/common/time_config"
        result = self.call(url, "GET", get_system_time=True)
        if result.get('result', {}).get("code") != 0:
            msg = (_('Get system time config session error. result: %(res)s.')
                   % {'res': result})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        if result.get("data"):
            return result.get("data")[0]
        return {}
