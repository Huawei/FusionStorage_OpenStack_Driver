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

import json

from oslo_log import log

from manila import exception
from manila.i18n import _

from .rest_client import RestClient
from ..utils import constants
from ..utils import driver_utils

LOG = log.getLogger(__name__)


class DMEClient(RestClient):
    """DMEClient class for OceanStorPacific storage system."""

    def __init__(self, driver_config):
        super(DMEClient, self).__init__(driver_config)
        self.base_url = self.driver_config.rest_url
        self.login_url = self.base_url + constants.DME_LOGIN_URL
        self.relogin_codes = constants.DME_RETRY_RELOGIN_CODE
        self.retry_codes = constants.DME_RETRY_CODE
        self.retry_times = constants.DME_REQUEST_RETRY_TIMES

    @staticmethod
    def get_total_info_by_offset(func, extra_param):
        """
        Call the func interface cyclically to obtain the information in "data",
        combine it into a list and return it.
        which is used in the paging query interface
        """

        offset = 1
        total_info = []
        while True:
            result = func(offset, extra_param)
            data_info = result.get("data", [])
            total_info = total_info + data_info
            if len(data_info) < constants.DME_GFS_MAX_PAGE_COUNT:
                break
            offset += 1
        return total_info

    @staticmethod
    def get_total_data_by_offset(func, extra_param):
        """
        Call the func interface cyclically to obtain the information,
        combine it into a list and return it.
        which is used in the paging query interface
        """
        offset = 1
        total_info = []
        while True:
            result = func(offset, extra_param)
            total_info = total_info + result
            if len(result) < constants.DME_GFS_MAX_PAGE_COUNT:
                break
            offset += 1
        return total_info

    @staticmethod
    def _error_code(res):
        """
        get http status code and
        error code from response body if exist
        :param res: response object
        :return: http code
        """
        status_code = res.status_code

        if status_code in constants.DME_HTTP_SUCCESS_CODE:
            error_code = constants.DME_REST_NORMAL
        else:
            result = res.json()
            error_code = result.get('error_code', status_code)
        LOG.debug("Response http code is %s, error_code is %s", status_code, error_code)
        return status_code, error_code

    @staticmethod
    def _assert_result(result, msg_format, special_error_code_param=None):
        """
        Check whether need to raise error or not
        if special error param exist, raise this error
        otherwise, raise common error InvalidRequest
        :param result: url response body
        :param msg_format: common error msg
        :param special_error_code_param: special error dict
        :return: raise error or None
        """

        error_code = result.get('error_code')
        if error_code == constants.DME_REST_NORMAL:
            LOG.debug("DME restful url calling normal")
            return

        if not special_error_code_param:
            msg = (msg_format + 'result: %s.') % result
            LOG.error(msg)
            raise exception.InvalidRequest(msg)

        if error_code in special_error_code_param.get('special_code'):
            error_msg = special_error_code_param.get('error_msg')
            LOG.info(error_msg)
            raise special_error_code_param.get('error_type')(error_msg)

    @staticmethod
    def _check_login_code(result):
        """
        if login failed, get and print the exception reason logs
        :param result: login response result
        :return:
        """
        error_code = result.get('error_code')
        if error_code == constants.DME_REST_NORMAL:
            return

        exception_id = result.get('exceptionId')
        LOG.error('Failed to login DME storage, reason is %s', exception_id)
        raise exception.InvalidRequest(reason=exception_id)

    def retry_relogin(self, old_token):
        """
        Add write lock when do re-login to
        hang up other business restful url
        :param old_token: the old session
        :return:
        """
        with self.call_lock.write_lock():
            self.relogin(old_token)

    def login(self):
        data = {
            "grantType": "password",
            "userName": self.driver_config.user_name,
            "value": self.driver_config.user_password
        }
        self.init_http_head()
        # do login
        LOG.info("Begin to login DME storage, the login url is %s", self.login_url)
        res = self._session.put(
            self.login_url, data=json.dumps(data),
            timeout=constants.DME_SOCKET_TIMEOUT,
            verify=self._session.verify
        )
        result = res.json()
        result['error_code'] = self._error_code(res)[1]

        self._check_login_code(result)

        self._session.headers.update({
            "X-Auth-Token": result.get('accessSession')
        })

        self.is_online = True
        LOG.info("Login the DME Storage success, login_url is %s" % self.login_url)
        return result

    def logout(self):
        if not self.is_online:
            return

        try:
            self.semaphore.acquire()
            self._session.delete(self.login_url, timeout=constants.SOCKET_TIMEOUT)
        except Exception as err:
            LOG.warning("Logout DME Client"
                        " failed because of %(reason)s".format(reason=err))
        finally:
            self.semaphore.release()
            self._session.close()
            self._session = None
            self.is_online = False
            LOG.info("Logout the DME Client success, logout_url is %s" % self.login_url)
        return

    def relogin(self, old_token):
        if (self._session and
                self._session.headers.get('X-Auth-Token') != old_token):
            LOG.info('Relogin has been done by other thread, '
                     'no need relogin again.')
            return {}

        self.logout()
        return self.login()

    def query_cluster_statistics_by_name(self, cluster_name):
        url = '/rest/fileservice/v1/gfs-groups/query-summary'
        query_param = {
            'name': cluster_name
        }
        result = self.call(url, data=query_param, method='POST')
        self._assert_result(result, "Query cluster classifications failed,")
        return result

    def create_gfs(self, gfs_param):
        url = '/rest/fileservice/v1/gfs'
        result = self.call(url, data=gfs_param, method='POST')
        self._assert_result(result, "Create GFS failed,")
        return result

    def create_gfs_dtree(self, gfs_dtree_param):
        url = '/rest/fileservice/v1/gfs/dtrees'
        result = self.call(url, data=gfs_dtree_param, method='POST')
        self._assert_result(result, "Create GFS Dtree failed,")
        return result

    def add_ipaddress_to_gfs(self, gfs_params):
        url = '/rest/fileservice/v1/gfs/dpc-auth-clients/add'
        result = self.call(url, data=gfs_params, method='POST')
        self._assert_result(result, "add the ip addresses of the dpc to the gfs failed,")
        return result

    def remove_ipaddress_from_gfs(self, gfs_params):
        url = '/rest/fileservice/v1/gfs/dpc-auth-clients/delete'
        result = self.call(url, data=gfs_params, method='POST')
        self._assert_result(result, "delete the ip addresses of the dpc to the gfs failed,")
        return result

    def change_gfs_size(self, modify_param):
        url = '/rest/fileservice/v1/gfs/modify'
        result = self.call(url, data=modify_param, method='POST')
        self._assert_result(result, "Change GFS size failed,")
        return result

    def change_gfs_quota_size(self, modify_param):
        url = '/rest/fileservice/v1/gfs/quotas/modify'
        result = self.call(url, data=modify_param, method='POST')
        self._assert_result(result, "Change GFS quota size failed,")
        return result

    def change_gfs_dtree_size(self, modify_param):
        url = '/rest/fileservice/v1/gfs/dtrees/quotas/modify'
        result = self.call(url, data=modify_param, method='POST')
        self._assert_result(result, "Change GFS dtree size failed,")
        return result

    def query_gfs_detail(self, name_locator):
        url = '/rest/fileservice/v1/gfs/detail/query'
        data = {
            "name_locator": name_locator
        }
        result = self.call(url, data=data, method='POST')
        self._assert_result(result, "Query GFS detail failed,")
        return result

    def query_gfs_dtree_detail(self, name_locator):
        url = '/rest/fileservice/v1/gfs/dtrees/detail-query'
        data = {
            "name_locator": name_locator
        }
        result = self.call(url, data=data, method='POST')
        if result.get("error_code") == 'common.0005':
            LOG.info("The object %s does not exist.", name_locator)
            return {}
        self._assert_result(result, "Query GFS dtree detail failed,")
        return result

    def query_task_by_id(self, task_id):
        url = '/rest/taskmgmt/v1/tasks/{0}'.format(task_id)
        result = self.call(url, None, method='GET')
        self._assert_result(result, "query task {0} failed,".format(task_id))

        # 获取任务信息，任务查询结果是个列表，里面有当前任务及其子任务
        task_list = result.get("data", [])
        root_task = None
        for task in task_list:
            if task.get("id") == task_id:
                root_task = task
                break
        if not root_task:
            msg = (_('query task failed, task not in task list, task id: {0}'.format(task_id)))
            LOG.error(msg)
            raise exception.InvalidShare(msg)

        return root_task

    def wait_task_until_complete(self, task_id, time_out_seconds=60 * 30, query_interval_seconds=3):
        def query_task_callback():
            task_info = self.query_task_by_id(task_id)

            # 任务状态，取值范围：1-初始状态;2-执行中;3-成功;4-部分成功;5-失败;6-超时
            task_status = task_info.get('status')
            if task_status in [1, 2]:
                # 1-初始状态;2-执行中，记录日志，等下个查询间隔
                LOG.info('task {0} status is: {1}, progress: {2}'
                         .format(task_id, task_status, task_info.get('progress')))
                return False
            elif task_status in [4, 5, 6]:
                # 4-部分成功;5-失败;6-超时，抛出异常
                msg = (_('task {0} complete but not success, status is: {1}'.format(task_id, task_status)))
                LOG.error(msg)
                raise exception.InvalidShare(reason=msg)
            elif task_status == 3:
                # 3-成功，反True
                return True
            else:
                # 其他情况，抛异常
                msg = (_('task {0} unknown status, status is: {1}'.format(task_id, task_status)))
                LOG.error(msg)
                raise exception.InvalidShare(reason=msg)

        driver_utils.wait_for_condition(query_task_callback, query_interval_seconds, time_out_seconds)

    def delete_gfs(self, gfs_delete_param):
        url = '/rest/fileservice/v1/gfs/delete'
        result = self.call(url, data=gfs_delete_param, method='POST')
        not_found_error_param = {
            'special_code': constants.GFS_NOT_EXIST,
            'error_msg': 'Delete gfs failed because of gfs not exist',
            'error_type': exception.ShareNotFound
        }
        self._assert_result(result, "Delete GFS failed,",
                            special_error_code_param=not_found_error_param)
        return result

    def delete_gfs_dtree(self, gfs_dtree_delete_param):
        url = '/rest/fileservice/v1/gfs/dtrees/delete'
        result = self.call(url, data=gfs_dtree_delete_param, method='POST')
        not_found_error_param = {
            'special_code': constants.GFS_DTREE_NOT_EXIST,
            'error_msg': 'Delete gfs dtree failed because of gfs or dtree not exist',
            'error_type': exception.ShareNotFound
        }
        self._assert_result(result, "Delete GFS Dtree failed,",
                            special_error_code_param=not_found_error_param)
        return result

    def get_gfs_info_by_name(self, gfs_query_param):
        url = '/rest/fileservice/v1/gfs/query'
        result = self.call(url, data=gfs_query_param, method='POST')
        self._assert_result(result, 'Query GFS info failed,')
        return result.get('data', [])

    def get_gfs_tier_migration_policies(self, query_param):
        url = '/rest/fileservice/v1/gfs/tier-migration-policies/query'
        result = self.call(url, data=query_param, method='POST')
        self._assert_result(result, 'Query GFS tier grade policies failed,')
        return result.get('data', [])

    def get_gfs_tier_grade_policies(self, query_param):
        url = '/rest/fileservice/v1/gfs/tier-placement-policies/query'
        result = self.call(url, data=query_param, method='POST')
        self._assert_result(result, 'Query GFS tier migration policies failed,')
        return result.get('data', [])

    def create_gfs_tier_migration_policy(self, create_param):
        url = '/rest/fileservice/v1/gfs/tier-migration-policies'
        result = self.call(url, data=create_param, method='POST')
        self._assert_result(result, 'Create GFS tier migration policy failed,')
        return result

    def create_gfs_tier_grade_policy(self, create_param):
        url = '/rest/fileservice/v1/gfs/tier-placement-policies'
        result = self.call(url, data=create_param, method='POST')
        self._assert_result(result, 'Create GFS tier grade policy failed,')
        return result

    def delete_gfs_tier_migration_policy(self, delete_param):
        url = '/rest/fileservice/v1/gfs/tier-migration-policies/delete'
        result = self.call(url, data=delete_param, method='POST')
        not_found_error_param = {
            'special_code': constants.GFS_TIER_POLICY_NOT_EXIST,
            'error_msg': 'Delete gfs tier migrate policy failed '
                         'because of object not exist',
            'error_type': exception.ShareNotFound
        }
        self._assert_result(result, 'Delete GFS tier migration policy failed,',
                            special_error_code_param=not_found_error_param)
        return result

    def delete_gfs_tier_grade_policy(self, delete_param):
        url = '/rest/fileservice/v1/gfs/tier-placement-policies/delete'
        result = self.call(url, data=delete_param, method='POST')
        not_found_error_param = {
            'special_code': constants.GFS_TIER_POLICY_NOT_EXIST,
            'error_msg': 'Delete gfs tier grade policy failed because of object not exist',
            'error_type': exception.ShareNotFound
        }
        self._assert_result(result, 'Delete GFS tier grade policy failed,',
                            special_error_code_param=not_found_error_param)
        return result

    def modify_gfs_tier_grade_policy(self, modify_param):
        url = '/rest/fileservice/v1/gfs/tier-placement-policies/modify'
        result = self.call(url, data=modify_param, method='POST')
        self._assert_result(result, 'Modify GFS tier grade policy failed,')
        return result

    def modify_gfs_tier_migrate_policy(self, modify_param):
        url = '/rest/fileservice/v1/gfs/tier-migration-policies/modify'
        result = self.call(url, data=modify_param, method='POST')
        self._assert_result(result, 'Modify GFS tier migrate policy failed,')
        return result

    def create_gfs_qos_policy(self, qos_param):
        url = '/rest/fileservice/v1/gfs/qos/create'
        result = self.call(url, data=qos_param, method='POST')
        self._assert_result(result, 'Create GFS qos policy failed, ')
        return result

    def query_gfs_qos_policy(self, param):
        url = '/rest/fileservice/v1/gfs/qos/query'
        result = self.call(url, data=param, method='POST')
        self._assert_result(result, 'Get gfs qos from gfs name failed,')
        return result.get('qos_list', [])

    def update_gfs_qos_policy(self, param):
        url = '/rest/fileservice/v1/gfs/qos/modify'
        result = self.call(url, data=param, method='POST')
        self._assert_result(result, 'Update gfs qos from gfs name failed,')
        return result

    def get_all_gfs_capacities_info(self, cluster_name):
        totals = self.get_total_info_by_offset(
            self._get_gfs_capacities_info, cluster_name)
        return totals

    def get_all_gfs_dtree_capacities_info(self, cluster_name):
        totals = self.get_total_info_by_offset(
            self._get_gfs_dtree_capacities_info, cluster_name)
        return totals

    def query_specified_file_system(self, param):
        file_systems = self.get_file_systems(param)
        if not file_systems or len(file_systems) != constants.DME_DATA_COUNT_ONE:
            err_msg = _("Expected at most 1 file system, but got {0}.").format(len(file_systems))
            raise exception.InvalidShare(reason=err_msg)

        return file_systems[0]

    def query_specified_dtree(self, param):
        dtrees = self.get_dtrees(param)
        if not dtrees or len(dtrees) != constants.DME_DATA_COUNT_ONE:
            err_msg = _("Expected at most 1 dtree, but got {0}.").format(len(dtrees))
            raise exception.InvalidShare(reason=err_msg)

        return dtrees[0]

    def query_specified_quota(self, param):
        quotas = self.get_quotas(param)
        if not quotas or len(quotas) != constants.DME_DATA_COUNT_ONE:
            err_msg = _("Expected at most 1 quota, but got {0}.").format(len(quotas))
            raise exception.InvalidShare(reason=err_msg)

        return quotas[0]

    def query_specified_pool(self, param):
        pools = self.get_storage_pools(param)
        if not pools or len(pools) != constants.DME_DATA_COUNT_ONE:
            err_msg = _("Expected at most 1 pool, but got {0}.").format(len(pools))
            raise exception.InvalidShare(reason=err_msg)

        return pools[0]

    def get_file_systems(self, param):
        return self.get_total_data_by_offset(self.get_file_systems_by_page, param)

    def get_file_systems_by_page(self, page_no, param):
        request = {
            'page_no': page_no,
            'page_size': constants.DME_GFS_MAX_PAGE_COUNT
        }
        request.update(param)
        url = '/rest/fileservice/v1/filesystems/query'
        result = self.call(url, data=request, method='POST')
        self._assert_result(result, 'get file system failed by page')
        return result.get('data', [])

    def get_file_system_by_name(self, param, name):
        file_systems = self.get_file_systems(param)
        if file_systems is None or len(file_systems) == 0:
            raise exception.InvalidShare(reason="Can not get file systems.")
        # 由于文件系统查询接口名称是模糊搜索,查询到的列表再通过 name 再精确过滤
        filtered_file_systems = [fs for fs in file_systems if fs.get('name') == name]
        # 检查过滤后的元素数量是否为 1
        if len(filtered_file_systems) != 1:
            raise ValueError("Expected 1 file system with name '{}', but found {}.".format(
                name, len(filtered_file_systems)
            ))
        return filtered_file_systems[0]

    def get_file_system_detail(self, fs_id):
        url = '/rest/fileservice/v1/filesystems/{0}'.format(fs_id)
        result = self.call(url, data=None, method='GET')
        not_found_error_param = {
            'special_code': constants.FILESYSTEM_NOT_EXIST,
            'error_msg': 'get file system detail failed because of filesystem not exist',
            'error_type': exception.ShareNotFound
        }
        self._assert_result(result, "Get file system detail failed,", special_error_code_param=not_found_error_param)
        return result

    def get_dtrees(self, param):
        return self.get_total_data_by_offset(self.get_dtrees_by_page, param)

    def get_dtrees_by_page(self, page_no, param):
        request = {
            'page_no': page_no,
            'page_size': constants.DME_GFS_MAX_PAGE_COUNT
        }
        request.update(param)
        url = '/rest/fileservice/v1/dtrees/query'
        result = self.call(url, data=request, method='POST')
        self._assert_result(result, 'get dtrees failed by page')
        return result.get('dtrees', [])

    def get_dtree_by_name_and_vstore(self, param, name, vstore_id):
        dtrees = self.get_dtrees(param)
        if dtrees is None or len(dtrees) == 0:
            raise exception.InvalidShare(reason="Can not get file dtrees.")
        # 由于Dtree查询接口名称是模糊搜索,查询到的列表再通过 name 再精确过滤
        filtered_dtrees = [dt for dt in dtrees if dt.get('name') == name and dt.get('vstore_id') == vstore_id]
        # 检查过滤后的元素数量是否为 1
        if len(filtered_dtrees) != 1:
            raise ValueError("Expected 1 dtree with name '{}', but found {}.".format(
                name, len(filtered_dtrees)
            ))
        return filtered_dtrees[0]

    def delete_nfs_share(self, nfs_share_ids):
        param = {
            'nfs_share_ids': nfs_share_ids
        }
        url = '/rest/fileservice/v1/nfs-shares/delete'
        result = self.call(url, data=param, method='POST')
        self._assert_result(result, 'delete nfs share failed')
        return result.get('task_id')

    def delete_dpc_share(self, dpc_share_ids):
        param = {
            'dpc_share_ids': dpc_share_ids
        }
        url = '/rest/fileservice/v1/dpc-shares/delete'
        result = self.call(url, data=param, method='POST')
        self._assert_result(result, 'delete dpc share failed')
        return result.get('task_id')

    def delete_file_system(self, file_system_ids):
        param = {
            'file_system_ids': file_system_ids
        }
        url = '/rest/fileservice/v1/filesystems/delete'
        result = self.call(url, data=param, method='POST')
        not_found_error_param = {
            'special_code': constants.FILESYSTEM_NOT_EXIST,
            'error_msg': 'Delete filesystem failed because of filesystem not exist',
            'error_type': exception.ShareNotFound
        }
        self._assert_result(result, "Delete file system failed,", special_error_code_param=not_found_error_param)
        return result.get('task_id')

    def delete_dtree(self, dtree_ids):
        param = {
            'dtree_ids': dtree_ids
        }
        url = '/rest/fileservice/v1/dtrees/delete'
        result = self.call(url, data=param, method='POST')
        not_found_error_param = {
            'special_code': constants.DME_DTREE_NOT_EXIST,
            'error_msg': 'Delete dtree failed because of dtree not exist',
            'error_type': exception.ShareNotFound
        }
        self._assert_result(result, "Delete dtree failed,", special_error_code_param=not_found_error_param)
        return result.get('task_id')

    def update_file_system(self, fs_id, param):
        url = '/rest/fileservice/v1/filesystems/{0}'.format(fs_id)
        result = self.call(url, data=param, method='PUT')
        not_found_error_param = {
            'special_code': constants.FILESYSTEM_NOT_EXIST,
            'error_msg': 'update file system failed because of file system not exist',
            'error_type': exception.ShareNotFound
        }
        self._assert_result(result, "update file system failed,", special_error_code_param=not_found_error_param)
        return result.get('task_id')

    def update_quota(self, quota_id, param):
        url = '/rest/fileservice/v1/quotas/{0}'.format(quota_id)
        result = self.call(url, data=param, method='PUT')
        not_found_error_param = {
            'special_code': constants.DME_QUOTA_NOT_EXIST,
            'error_msg': 'update quota failed because of quota not exist',
            'error_type': exception.ShareNotFound
        }
        self._assert_result(result, "update file system failed,", special_error_code_param=not_found_error_param)
        return result.get('task_id')

    def get_quotas(self, param):
        return self.get_total_data_by_offset(self.get_quotas_by_page, param)

    def get_quotas_by_page(self, page_no, param):
        request = {
            'page_no': page_no,
            'page_size': constants.DME_GFS_MAX_PAGE_COUNT
        }
        request.update(param)
        url = '/rest/fileservice/v1/quotas/query'
        result = self.call(url, data=request, method='POST')
        self._assert_result(result, 'get quotas failed')
        return result.get('datas', [])

    def get_storage_pools(self, param):
        return self.get_total_data_by_offset(self.get_storage_pools_by_page, param)

    def get_storage_pools_by_page(self, page_no, param):
        request = {
            'page_no': page_no,
            'page_size': constants.DME_GFS_MAX_PAGE_COUNT
        }
        request.update(param)
        zone_id = param.get('zone_id')
        if zone_id is not None and zone_id:
            url = '/rest/storagemgmt/v1/storagepools/query'
        else:
            url = '/rest/storagemgmt/v1/hyperscale-pools/query'
        result = self.call(url, data=request, method='POST')
        self._assert_result(result, 'get storage pools failed by page')
        return result.get('datas', result.get('data', []))

    def get_qos(self, param):
        return self.get_total_data_by_offset(self.get_qos_by_page, param)

    def get_qos_by_page(self, page_no, param):
        request = {
            'page_no': page_no,
            'page_size': constants.DME_GFS_MAX_PAGE_COUNT
        }
        request.update(param)
        url = '/rest/storagepolicy/v1/qos/query'
        result = self.call(url, data=request, method='POST')
        self._assert_result(result, 'get qos failed')
        return result.get('datas', [])

    def create_quota(self, param):
        result = self.call('/rest/fileservice/v1/quotas', data=param, method='POST')
        self._assert_result(result, "Create quota failed")
        return result.get('task_id')

    def create_dtree(self, dtree_param):
        result = self.call('/rest/fileservice/v1/dtrees', data=dtree_param, method='POST')
        self._assert_result(result, "Create dtree failed")
        return result.get('task_id')

    def create_file_system(self, param):
        url = '/rest/fileservice/v1/filesystems/customize-filesystems'
        result = self.call(url, data=param, method='POST')
        LOG.info("call create fs, the result is %s", result)
        self._assert_result(result, "create file system failed")
        return result.get('task_id')

    def get_storages(self, param):
        return self.get_total_data_by_offset(self.get_get_storages_by_page, param)

    def get_get_storages_by_page(self, page_no, param):
        url = '/rest/storagemgmt/v1/storages?start=%d&limit=1000' % page_no
        result = self.call(url, method='GET')
        self._assert_result(result, 'get storage failed by page')
        return result.get('datas', [])

    def get_cluster_zones(self, storage_id):
        param = {"storage_ids": [storage_id]}
        url = '/rest/storageclusterservice/v1/zones/query'
        result = self.call(url, data=param, method='POST')
        self._assert_result(result, 'get zones failed by storage_id')
        return result.get('datas', [])

    def get_vstores(self, param):
        return self.get_total_data_by_offset(self.get_vstores_by_page, param)

    def get_vstores_by_page(self, page_no, param):
        request = {
            'page_no': page_no,
            'page_size': constants.DME_GFS_MAX_PAGE_COUNT
        }
        request.update(param)
        url = '/rest/fileservice/v1/vstores/query'
        result = self.call(url, data=request, method='POST')
        self._assert_result(result, 'get vstores failed by page')
        return result.get('vstores', [])

    def get_dpc_administrators(self, param):
        return self.get_total_data_by_offset(self.get_dpc_administrators_by_page, param)

    def get_dpc_administrators_by_page(self, page_no, param):
        request = {
            'page_no': page_no,
            'page_size': constants.DME_GFS_MAX_PAGE_COUNT
        }
        request.update(param)
        url = '/rest/fileservice/v1/dpc-administrators/query'
        result = self.call(url, data=request, method='POST')
        self._assert_result(result, 'get dpc administrators failed by page')
        return result.get('administrators', [])

    def delete_qos(self, qos_ids):
        param = {
            'ids': qos_ids
        }
        url = '/rest/storagepolicy/v1/qos/delete'
        result = self.call(url, data=param, method='POST')
        not_found_error_param = {
            'special_code': constants.FILESYSTEM_NOT_EXIST,
            'error_msg': 'Delete qos failed because of qos not exist',
            'error_type': exception.ShareNotFound
        }
        self._assert_result(result, "Delete qos failed,", special_error_code_param=not_found_error_param)
        return result.get('task_id')

    def update_nfs_share(self, share_id, param):
        url = '/rest/fileservice/v2/nfs-shares/{0}'.format(share_id)
        result = self.call(url, data=param, method='PUT')
        self._assert_result(result, 'update nfs share failed')
        return result.get('task_id')

    def allow_access_for_nfs(self, share_id, access_to, access_level):
        access_value = 'read' if access_level == 'ro' else 'read_and_write'
        param = {
            "nfs_share_client_addition": [
                {
                    "name": access_to,
                    "permission": access_value,
                    "write_mode": "synchronization",
                    "permission_constraint": "no_all_squash",
                    "root_permission_constraint": "no_root_squash",
                    "source_port_verification": "insecure"
                }
            ],
            "character_encoding": self.driver_config.nfs_charset
        }

        return self.update_nfs_share(share_id, param)

    def deny_access_for_nfs(self, share_id, access_to, nfs_share_client_id):
        param = {
            "description": "",
            "nfs_share_client_addition": [],
            "nfs_share_client_modification": [],
            "nfs_share_client_deletion": [
                {
                    "name": access_to,
                    "nfs_share_client_id_in_storage": nfs_share_client_id
                }
            ],
            "character_encoding": self.driver_config.nfs_charset,
            "show_snapshot_enable": True
        }

        return self.update_nfs_share(share_id, param)

    def get_nfs_share(self, param):
        return self.get_total_data_by_offset(self.get_nfs_share_by_page, param)

    def get_nfs_share_by_page(self, page_no, param):
        request = {
            'page_no': page_no,
            'page_size': constants.DME_GFS_MAX_PAGE_COUNT
        }
        request.update(param)
        url = '/rest/fileservice/v1/nfs-shares/query'
        result = self.call(url, data=request, method='POST')
        self._assert_result(result, 'get nfs share failed by page')
        return result.get('nfs_share_info_list', [])

    def get_dpc_share(self, param):
        return self.get_total_data_by_offset(self.get_dpc_share_by_page, param)

    def get_dpc_share_by_page(self, page_no, param):
        request = {
            'page_no': page_no,
            'page_size': constants.DME_GFS_MAX_PAGE_COUNT
        }
        request.update(param)
        url = '/rest/fileservice/v1/dpc-shares/query'
        result = self.call(url, data=request, method='POST')
        self._assert_result(result, 'get dpc share failed by page')
        return result.get('data', [])

    def get_nfs_share_clients(self, param):
        return self.get_total_data_by_offset(self.get_nfs_share_clients_by_page, param)

    def get_nfs_share_clients_by_page(self, page_no, param):
        request = {
            'page_no': page_no,
            'page_size': constants.DME_GFS_MAX_PAGE_COUNT
        }
        request.update(param)
        url = '/rest/fileservice/v2/nfs-auth-clients/query'
        result = self.call(url, data=request, method='POST')
        self._assert_result(result, 'get nfs share clients failed by page')
        return result.get('auth_client_list', [])

    def _get_gfs_capacities_info(self, offset, cluster_name):
        gfs_query_param = {
            'cluster_classification_name': cluster_name,
            'page_no': offset,
            'page_size': constants.DME_GFS_MAX_PAGE_COUNT
        }
        url = '/rest/fileservice/v1/gfs/capacities/query'
        result = self.call(url, data=gfs_query_param, method='POST')
        self._assert_result(result, 'Get GFS capacities info failed,')
        return result

    def _get_gfs_dtree_capacities_info(self, offset, cluster_name):
        dtree_query_param = {
            'cluster_classification_name': cluster_name,
            'page_no': offset,
            'page_size': constants.DME_GFS_MAX_PAGE_COUNT
        }
        url = '/rest/fileservice/v1/gfs/dtrees/capacities/query'
        result = self.call(url, data=dtree_query_param, method='POST')
        self._assert_result(result, 'Get Dtrees capacities info failed,')
        return result