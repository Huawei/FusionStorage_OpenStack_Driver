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

import requests
import six
from oslo_log import log as logging
from requests.adapters import HTTPAdapter

from cinder import exception
from cinder.i18n import _
from cinder.volume.drivers.fusionstorage import constants

LOG = logging.getLogger(__name__)


class HostNameIgnoringAdapter(HTTPAdapter):
    def cert_verify(self, conn, url, verify, cert):
        conn.assert_hostname = False
        return super(HostNameIgnoringAdapter, self).cert_verify(
            conn, url, verify, cert)


class RestCommon(object):
    def __init__(self, fs_address, fs_user, fs_password, **extend_conf):
        self.address = fs_address
        self.user = fs_user
        self.password = fs_password

        self.session = None
        self.token = None
        self.version = None
        self.esn = None
        mutual_authentication = extend_conf.get("mutual_authentication", {})
        self.init_http_head(mutual_authentication)

        LOG.warning("Suppressing requests library SSL Warnings")
        requests.packages.urllib3.disable_warnings(
            requests.packages.urllib3.exceptions.InsecureRequestWarning)
        requests.packages.urllib3.disable_warnings(
            requests.packages.urllib3.exceptions.InsecurePlatformWarning)

    def init_http_head(self, mutual_authentication=None):
        self.session = requests.Session()
        self.session.headers.update({
            "Content-Type": "application/json;charset=UTF-8",
        })
        self.session.verify = False
        self.session.mount(self.address, HostNameIgnoringAdapter())

        if mutual_authentication.get("storage_ssl_two_way_auth"):
            self.session.verify = \
                mutual_authentication.get("storage_ca_filepath")
            self.session.cert = \
                (mutual_authentication.get("storage_cert_filepath"),
                 mutual_authentication.get("storage_key_filepath"))

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

    @staticmethod
    def _assert_rest_result(result, err_str):
        if isinstance(result.get('result'), dict):
            if result['result'].get("code") != 0:
                msg = (_('%(err)s\nresult: %(res)s.') % {'err': err_str,
                                                         'res': result})
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)
        elif result.get('result') != 0:
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

    def get_esn(self):
        url = "/cluster/sn"
        result = self.call(url, "get")
        self._assert_rest_result(result, _('Get cluster esn error.'))
        self.esn = result.get("sn")
        return self.esn

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
        self.get_esn()

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

    def query_storage_pool_info(self):
        url = "/cluster/storagepool/queryStoragePool"
        result = self.call(url, 'GET', get_version=True, filter_flag=True)
        self._assert_rest_result(result, _("Query pool session error."))
        return result.get('storagePools', [])

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

    def query_volume_by_name_v2(self, vol_name):
        url = '/api/v2/block_service/volumes?name=' + vol_name
        result = self.call(url, 'GET', get_system_time=True)
        if result.get('errorCode') in constants.VOLUME_NOT_EXIST:
            return {}
        self._assert_rest_result(
            result, _("Query volume by name session error"))
        return result.get('data', {})

    def query_volume_by_id(self, vol_id):
        url = 'v1.3/volume/queryById?volId=' + vol_id
        result = self.call(url, 'GET', get_version=True)
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
        if result.get('errorCode') in constants.VOLUME_NOT_EXIST:
            return None
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
        url = '/volume/detach'
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
        url = '/snapshot/create'
        params = {"volName": vol_name, "snapshotName": snapshot_name}
        result = self.call(url, "POST", params)
        self._assert_rest_result(result, _('Create snapshot error.'))

    def delete_snapshot(self, snapshot_name):
        url = '/snapshot/delete'
        params = {"snapshotName": snapshot_name}
        result = self.call(url, "POST", params)
        if result.get('errorCode') in constants.SNAPSHOT_NOT_EXIST:
            return None
        self._assert_rest_result(result, _('Delete snapshot session error.'))

    def create_volume_from_snapshot(self, snapshot_name, vol_name, vol_size):
        url = '/snapshot/volume/create'
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

    @staticmethod
    def _is_detail_error(result, detail_error_code):
        if result.get("result", "") == constants.DSWARE_MULTI_ERROR:
            for err in result.get("detail", []):
                if err.get("errorCode") == detail_error_code:
                    return True
            return False
        return True

    def create_host(self, host_name):
        url = '/host/create'
        params = {"hostName": host_name}
        result = self.call(url, "POST", params)
        if self._is_detail_error(result, constants.HOST_ALREADY_EXIST):
            return None

        self._assert_rest_result(result, _('Create host session error.'))

    def delete_host(self, host_name):
        url = '/host/delete'
        params = {"hostName": host_name}
        result = self.call(url, "POST", params)
        if self._is_detail_error(result, constants.HOST_MAPPING_EXIST):
            return None

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
        if self._is_detail_error(result, constants.HOSTGROUP_ALREADY_EXIST):
            return None
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
        if self._is_detail_error(result, constants.HOST_MAPPING_GROUP_EXIST):
            return None

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
        if self._is_detail_error(result, constants.INITIATOR_ALREADY_EXIST):
            return None
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
        if self._is_detail_error(result, constants.INITIATOR_IN_HOST):
            associate_host_ini = self.get_associate_initiator_by_host_name(
                host_name)
            if initiator in associate_host_ini:
                return None
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

    def get_snapshot_info_by_name(self, snapshot_name):
        url = "/api/v2/block_service/snapshots"
        params = {"name": snapshot_name}
        result = self.call(url, "GET", params, get_system_time=True)
        self._assert_rest_result(
            result, _("Get snapshot info session error."))
        return result.get("data", {})

    def rollback_snapshot(self, vol_name, snapshot_name):
        url = "/snapshot/rollback"
        params = {"snapshotName": snapshot_name,
                  "volumeName": vol_name
                  }
        result = self.call(url, "POST", params)
        self._assert_rest_result(
            result, _("Rollback snapshot session error."))

    def cancel_rollback_snapshot(self, snapshot_name):
        url = "/api/v2/block_service/snapshots"
        params = {"name": snapshot_name,
                  "action": "cancel_rollback"
                  }
        result = self.call(url, "POST", params, get_system_time=True)
        self._assert_rest_result(
            result, _("Cancel rollback snapshot session error."))

    def get_iscsi_portal(self):
        url = "/dsware/service/cluster/dswareclient/queryIscsiPortal"
        result = self.call(url, "POST", data={}, get_system_time=True)
        self._assert_rest_result(
            result, _("Get ISCSI portal session error."))
        return result.get("nodeResultList", [])

    def get_host_iscsi_service(self, host_name):
        url = "/api/v2/block_service/iscsi_sessions"
        params = {"host_name": host_name}
        result = self.call(url, "GET", params, get_system_time=True)
        self._assert_rest_result(
            result, _("Get host iscsi service session error."))
        return result.get("data", [])

    def add_iscsi_host_relation(self, host_name, ip_list):
        if not ip_list:
            return
        url = "/dsware/service/iscsi/addIscsiHostRelation"
        ip_strings = ";".join(ip_list)
        params = [{"content": ip_strings, "key": host_name, "flag": 0}]
        try:
            result = self.call(url, "POST", params, get_system_time=True)
            if result.get("errorCode") == constants.HOST_ISCSI_RELATION_EXIST:
                result = self.get_iscsi_host_relation(host_name)
                if result:
                    return None
            self._assert_rest_result(
                result, _("Add iscsi host relation session error."))
        except Exception as err:
            if constants.URL_NOT_FOUND in six.text_type(err):
                return None
            else:
                raise

    def _get_iscsi_host_relation(self, key):
        url = "/dsware/service/iscsi/queryIscsiHostRelation"
        params = [{"key": key, "flag": 0}]
        try:
            result = self.call(url, "POST", params, get_system_time=True)
            self._assert_rest_result(
                result, _("Get iscsi host relation session error."))

            return result
        except Exception as err:
            if constants.URL_NOT_FOUND in six.text_type(err):
                return {}
            else:
                raise

    def get_iscsi_host_relation(self, host_name):
        result = self._get_iscsi_host_relation(host_name)
        iscsi_ips = []
        for iscsi in result.get("hostList", []):
            if int(iscsi.get("flag")) == constants.HOST_FLAG:
                iscsi_ips = iscsi.get("content", "").split(";")
        return iscsi_ips

    def delete_iscsi_host_relation(self, host_name, ip_list):
        if not ip_list:
            return

        url = "/dsware/service/iscsi/deleteIscsiHostRelation"
        ip_strings = ";".join(ip_list)
        params = [{"content": ip_strings, "key": host_name, "flag": 0}]
        try:
            result = self.call(url, "POST", params, get_system_time=True)
            self._assert_rest_result(
                result, _("Delete iscsi host relation session error."))
        except Exception as err:
            if constants.URL_NOT_FOUND in six.text_type(err):
                return None
            else:
                raise

    def get_iscsi_links_info(self, iscsi_link_count, pool_list):
        iscsi_ips = []
        url = "/dsware/service/iscsi/queryVbsIscsiLinks"
        params = {"amount": iscsi_link_count,
                  "poolList": pool_list}
        try:
            result = self.call(url, "POST", params, get_system_time=True)
            self._assert_rest_result(
                result, _("Get iscsi host relation session error."))
        except Exception as err:
            if constants.URL_NOT_FOUND in six.text_type(err):
                return iscsi_ips
            else:
                raise

        return [iscsi["ip"] for iscsi in result.get("iscsiLinks", [])
                if iscsi.get("ip")]

    def create_lun_migration(self, src_lun_id, dst_lun_id, speed=2):
        url = "/api/v2/block_service/lun_migration"
        params = {
            "parent_id": src_lun_id,
            "target_lun_id": dst_lun_id,
            "speed": speed,
            "work_mode": 0
        }
        result = self.call(url, "POST", params, get_system_time=True)
        self._assert_rest_result(result,
                                 _("create lun migration task error."))

    def get_lun_migration_task_by_id(self, src_lun_id):
        url = "/api/v2/block_service/lun_migration"
        params = {
            "id": src_lun_id
        }
        result = self.call(url, "GET", params, get_system_time=True)
        self._assert_rest_result(result,
                                 _("get lun migration task error."))
        return result.get('data', {})

    def delete_lun_migration(self, src_lun_id):
        url = "/api/v2/block_service/lun_migration"
        params = {
            'id': src_lun_id
        }
        result = self.call(url, "DELETE", params, get_system_time=True)
        self._assert_rest_result(result,
                                 _("Delete lun migration task error."))

    def get_volume_snapshot(self, volume_name):
        url = "/volume/snapshot/list"
        params = {
            'volName': volume_name,
            'batchNum': 1,
            'batchLimit': 10
        }
        result = self.call(url, "POST", params)
        return result.get('snapshotList', [])

    def create_consistent_snapshot_by_name(self, snapshot_group_list):
        url = "/api/v2/block_service/consistency_snapshots"
        result = self.call(url, "POST", snapshot_group_list, get_system_time=True)
        self._assert_rest_result(result, _("create consistent_snapshot error."))
        return result.get('data', [])

    def create_full_volume_from_snapshot(self, vol_name, snapshot_name):
        url = '/api/v2/block_service/createFullVolumeFromSnap'
        params = {"snap_name_src": snapshot_name, "volume_name_dst": vol_name}
        result = self.call(url, "POST", params, get_system_time=True)
        self._assert_rest_result(
            result, _("create full volume from snap fails"))

    def is_support_links_balance_by_pool(self):
        result = self._get_iscsi_host_relation('get_newiscsi')
        if result.get("newIscsi"):
            LOG.info("Support new iscsi interface to get iscsi ip.")
            return True
        return False

    def get_iscsi_links_by_pool(self, iscsi_link_count, pool_name, host_name):
        url = "/dsware/service/iscsi/queryIscsiLinks"
        params = {
            "amount": iscsi_link_count,
            "poolList": [pool_name],
            "hostKey": host_name
        }

        try:
            result = self.call(url, "POST", params, get_system_time=True)
            self._assert_rest_result(
                result, _("Get iscsi links by pool error."))
            return result
        except Exception as err:
            if constants.URL_NOT_FOUND in six.text_type(err):
                return {}
            else:
                raise
