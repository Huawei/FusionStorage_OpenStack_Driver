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
 Volume api for FusionStorage systems.
"""
import random
import re
import subprocess
import time
from oslo_log import log as logging

from cinder import utils

LOG = logging.getLogger(__name__)
fsc_cli = "fsc_cli"
fsc_port = '10519'
CMD_BIN = fsc_cli + ' '
MAX_NUM_OF_IP = 3

volume_info = {
    'result': 1,
    'vol_name': '',
    'father_name': '',
    'status': '',
    'vol_size': '',
    'real_size': '',
    'pool_id': '',
    'create_time': '',
    'lun_id': '',
    'wwn': ''
}

snap_info = {
    'result': 1,
    'snap_name': '',
    'father_name': '',
    'status': '',
    'snap_size': '',
    'real_size': '',
    'pool_id': '',
    'delete_priority': '',
    'create_time': '',
    'wwn': ''
}

pool_info = {
    'result': 1,
    'pool_id': '',
    'total_capacity': '',
    'used_capacity': '',
    'alloc_capacity': ''
}

qos_info = {
    'result': 1,
    'para_num': '',
    'qos_name': '',
    'max_iops': '',
    'max_mbps': '',
    'burst_iops': '',
    'credit_iops': '',
    'iops_perGB': '',
    'min_base_line_iops': '',
    'read_limit_iops': '',
    'read_limit_mbps': '',
    'write_limit_iops': '',
    'write_limit_mbps': '',
    'burst_mbps_perTB': '',
    'mbps_perTB': '',
    'min_base_line_mbps': ''
}

volume_attach_info = {
    'result': 1,
    'devName': ''
}

error_code_no_retry_list = {
    '50150006': 'SNAPSHOT_NOT_EXIST', '50150005': 'VOLUME_NOT_EXIST',
    '50151002': "VOLUME_IS_DELETINGz",
    '50090153': 'POOL_FAULTY', '50150003': 'CMD_FORMAT_ERROR',
    '50150009': 'SPACE_NOT_ENOUGH', '50150010': 'NODE_TYPE_ERROR',
    '50150011': 'VOLUME_AND_SNAPSHOT_BEYOND_MAX',
    '50150013': 'NODE_REFERENCE_LARGE_ZERO', '50150021': 'POOL_NOT_EXIST',
    '50151001': 'VOLUME_NAME_INVALID', '50151003': 'VOLUME_SIZE_INVALID',
    '50151005': 'VOLUME_IS_DUPLICATING',
    '50151006': 'BITMAP_VOLUME_IS_CREATING', '50151007': 'NODE_DATA_IN_BAD_SECTOR',
    '50151009': 'POOL_ID_IS_OUT_OF_RANGE', '50151010': 'POOL_ID_IS_NOT_USE',
    '50151012': 'STORAGE_POOL_SOME_NOT_READY', '50151015': 'POOL_IN_EMERGENCY_MODE',
    '50151201': 'SNAPSHOT_NAME_INVALID', '50151203': 'SNAPSHOT_IS_DUPLICATING',
    '50151207': 'CREATE_SNAP_NODE_TYPE_INVALID',
    '50151211': 'VOLUME_IS_MIGRATING',
    '50151216': 'VOLUME_TYPE_IS_NOT_MATCH', '50151401': 'VOLUME_HAS_ATTACHED',
    '50151402': 'ATTACHED_BEYOND_MAX',
    '50151403': 'NODE_NAME_INVALID', '50151404': 'VOLUME_SNAPSHOT_NOT_EXIST',
    '50151406': 'NODE_IS_ISCSI',
    '50151409': 'SHARE_VOL_REACH_MAX', '50151601': 'VOLUME_NOT_ATTACH',
    '50152203': 'DUPLICATE_SLAVE_VOLUME_IS_ASYNC',
    '50152401': 'SNAP_CHILD_TOO_MUCH',
    '50152501': 'DUPLICATE_SNAP_VOL_EXCEED_LIMIT', '50152502': 'SNAPSHOT_BEYOND_MAX',
    '50152601': 'NOT_VOLUME_NODE', '50153012': 'BRANCH_LEVEL_BEYOND_MAX',
    '50153014': 'SNAP_VOLUME_RELATION_ERROR',
    '50155009': 'SYNC_METADATA_ERROR', '50157010': 'VOLUME_HAS_MAPPING',
    '50510012': 'VBS_CLIENT_CMD_FORMAT_ERROR'
}
error_code_success_list = {
    '50150007': 'VOLUME_ALREADY_EXIST',
    '50150008': 'SNAPSHOT_ALREADY_EXIST', '50157019': 'ISCSI_HOST_ALREADY_EXISTED',
    '50157044': 'ISCSI_HOSTGROUP_ALREADY_EXISTED', '50155102': 'ISCSI_PORT_ALREADY_EXIST',
    '50157021': 'ISCSI_HOST_PORT_RELATION_ALREADY_EXISTED',
    '50157001': 'VOLUME_ALREADY_EXIST_IN_MAPPING',
    '50157046': 'ISCSI_HOSTGROUP_HOST_RELATION_ALREADY_EXISTED'
}


class FSPythonApi(object):
    def __init__(self, dsware_manager, fusionstorageagent):
        LOG.debug("FSPythonApi init")
        self.res_idx = len('result=')
        self.dsware_manager = dsware_manager
        self.fusionstorageagent = fusionstorageagent.split(',')

    def get_ip_port(self):
        return self.fusionstorageagent

    def get_manage_ip(self):
        return self.dsware_manager

    def start_api_server(self):
        # create dsware-api Jar daemon process
        cmd = CMD_BIN + "--op startServer"
        LOG.info("start_api_server cmd:%s", cmd)
        cmd_end = tuple(cmd.split())
        subprocess. \
            Popen(cmd_end)
        time.sleep(3)

        LOG.info("FSPythonApi starts api server end.")

    def result_cmd_is_special(self, exec_result):
        error_info = exec_result[self.res_idx:]
        error_infos = re.split(',', error_info)
        error_code = error_infos[0]
        if error_code in error_code_success_list:
            return True
        else:
            return False

    def execute_cmd_with_result(self, cmd, ip_list):
        manage_ip = self.get_manage_ip()
        exec_result = ""
        LOG.debug("execute cmd %s with result in %s" % (cmd, ip_list))
        for ip in ip_list:
            cmd_args = CMD_BIN + cmd + ' --manage_ip ' + manage_ip.replace(
                '\n', '') + ' --ip ' + ip.replace('\n', '')
            cmd_end = tuple(cmd_args.split())
            exec_result, err = utils.execute(*cmd_end, run_as_root=True)
            exec_result = exec_result.split('\n')
            LOG.info("DSWARE query cmd[%s] result is %s" % (cmd, exec_result))
            if not exec_result:
                return exec_result
            for line in exec_result:
                if not re.search('^result=', line):
                    continue
                result = line
                error_info = result[self.res_idx:]
                error_infos = re.split(',', error_info)
                error_code = error_infos[0]
                if re.search('^result=0', line):
                    return exec_result
                if error_code in error_code_success_list:
                    LOG.error("query cmd return error with success")
                    return 'result=0'
                elif error_code in error_code_no_retry_list:
                    return exec_result
                elif re.search('^result=5', line):
                    continue
        LOG.error("execute_cmd_with_result end without return")
        return exec_result

    def execute_cmd(self, cmd, ip_list):
        manage_ip = self.get_manage_ip()
        result = ""
        LOG.debug("execute cmd %s in %s" % (cmd, ip_list))
        for ip in ip_list:
            cmd_args = CMD_BIN + cmd + ' --manage_ip ' + manage_ip.replace(
                '\n', '') + ' --ip ' + ip.replace('\n', '')
            cmd_end = tuple(cmd_args.split())
            exec_result, err = utils.execute(*cmd_end, run_as_root=True)
            exec_result = exec_result.split('\n')
            LOG.info("DSWARE cmd[%s] result is %s" % (cmd, exec_result))
            if not exec_result:
                return result
            for line in exec_result:
                if not re.search('^result=', line):
                    continue
                result = line
                error_info = result[self.res_idx:]
                error_infos = re.split(',', error_info)
                error_code = error_infos[0]
                if re.search('^result=0', line):
                    return result
                if error_code in error_code_success_list:
                    return 'result=0'
                elif error_code in error_code_no_retry_list:
                    return result
                elif re.search('^result=5', line):
                    continue
        LOG.error("execute_cmd end without return")
        return result

    def start_execute_cmd(self, cmd, type_flag):
        fsc_ip = self.get_ip_port()
        ip_num = len(fsc_ip)
        random.shuffle(fsc_ip)
        if ip_num <= 0:
            return None
        ip_list = []
        ip_str = ""
        for index, ip in enumerate(fsc_ip, 1):
            if ip_str:
                ip_str = ip_str + "," + ip
            else:
                ip_str = ip
            if index % 1 == 0:
                ip_list.append(ip_str)
                ip_str = ""
        if ip_str:
            ip_list.append(ip_str)
        if type_flag:
            return self.execute_cmd_with_result(cmd, ip_list)
        else:
            return self.execute_cmd(cmd, ip_list)

    def start_execute_cmd_with_ip(self, cmd, target_ip):
        ip_list = [target_ip]
        return self.execute_cmd_with_result(cmd, ip_list)

    def start_execute_cmd_to_all(self, cmd):
        fsc_ip = self.get_ip_port()
        ip_num = len(fsc_ip)
        random.shuffle(fsc_ip)
        if ip_num <= 0:
            return None
        ip_list = []
        ip_str = ""
        for index, ip in enumerate(fsc_ip, 1):
            if ip_str:
                ip_str = ip_str + "," + ip
            else:
                ip_str = ip
            if index % 1 == 0:
                ip_list.append(ip_str)
                ip_str = ""
        if ip_str:
            ip_list.append(ip_str)
        return self.execute_cmd(cmd, ip_list)

    def get_lazyloading_count(self, identityString):
        cmd = '--op queryImgUsingVolNum' + ' ' + '--imageUUID' + ' ' + str(
            identityString)
        exec_result = self.start_execute_cmd(cmd, 1)
        if exec_result:
            for line in exec_result:
                if re.search('^result=', line):
                    if not re.search('^result=0', line):
                        return -1
                    for item_line in exec_result:
                        if re.search('vol_num=', item_line):
                            prefix = len('vol_num=')
                            return int(item_line[prefix:])
        else:
            return -1

    def create_LLD_volume(self, volume_name, pool_id, volume_size, isThin,
                          image_id, dataSrcUrl, image_size, offset, cacheFlag,
                          is_encrypted, cmkId, volume_auth_token, replace):
        cmd = '--op createLLDVolume' + ' ' + '--volName' + ' ' + str(
            volume_name) + ' ' + '--poolId' + ' ' + str(
            pool_id) + ' ' + '--volSize' + ' ' + str(
            volume_size) + ' ' + '--thinFlag' + ' ' + str(
            isThin) + ' ' + '--imageUUID' + ' ' + str(
            image_id) + ' ' + '--dataSrcURL' + ' ' + str(
            dataSrcUrl) + ' ' + '--imageSize' + ' ' + str(
            image_size) + ' ' + '--imageOffset' + ' ' + str(
            offset) + ' ' + '--cacheFlag' + ' ' + str(
            cacheFlag) + ' ' + '--replace' + ' ' + str(replace)

        if is_encrypted is not None and cmkId is not None and \
                volume_auth_token is not None:
            cmd = cmd + ' ' + '--encrypted' + ' ' + str(
                is_encrypted) + ' ' + '--cmkId' + ' ' + str(
                cmkId) + ' ' + '--authCredentials' + ' ' + str(
                volume_auth_token)

        exec_result = self.start_execute_cmd(cmd, 0)
        if exec_result:
            if re.search('^result=0', exec_result):
                return 0
            else:
                return exec_result[self.res_idx:]
        else:
            return 1

    def create_volume(self, vol_name, pool_id, vol_size, thin_flag,
                      is_encrypted=None, volume_cmkId=None,
                      volume_auth_token=None):
        cmd = '--op createVolume' + ' ' + '--volName' + ' ' + str(
            vol_name) + ' ' + '--poolId' + ' ' + str(
            pool_id) + ' ' + '--volSize' + ' ' + str(
            vol_size) + ' ' + '--thinFlag' + ' ' + str(thin_flag)

        if is_encrypted is not None and volume_cmkId is not None and \
                volume_auth_token is not None:
            cmd = cmd + ' ' + '--encrypted' + ' ' + str(
                is_encrypted) + ' ' + '--cmkId' + ' ' + str(
                volume_cmkId) + ' ' + '--authCredentials' + ' ' + str(
                volume_auth_token)

        exec_result = self.start_execute_cmd(cmd, 0)
        if exec_result:
            if re.search('^result=0', exec_result):
                return 0
            else:
                return exec_result[self.res_idx:]
        else:
            return 1

    def extend_volume(self, vol_name, new_vol_size):
        cmd = '--op expandVolume' + ' ' + '--volName' + ' ' + str(
            vol_name) + ' ' + '--volSize' + ' ' + str(new_vol_size)

        exec_result = self.start_execute_cmd(cmd, 0)
        if exec_result:
            if re.search('^result=0', exec_result):
                return 0
            else:
                return exec_result[self.res_idx:]
        else:
            return 1

    def create_volume_from_snap(self, vol_name, vol_size, snap_name):
        cmd = '--op createVolumeFromSnap' + ' ' + '--volName' + ' ' + str(
            vol_name) + ' ' + '--snapNameSrc' + ' ' + str(
            snap_name) + ' ' + '--volSize' + ' ' + str(vol_size)

        exec_result = self.start_execute_cmd(cmd, 0)
        if exec_result:
            if re.search('^result=0', exec_result):
                return 0
            else:
                return exec_result[self.res_idx:]
        else:
            return 1

    def migrate_volume(self, vol_name, dst_pool_id):
        cmd = '--op migrateVolumeCold' + ' ' + '--volNameSrc' + ' ' + \
              str(vol_name) + ' ' + '--poolIdDst' + ' ' + str(dst_pool_id)
        exec_result = self.start_execute_cmd(cmd, 0)
        if exec_result:
            if re.search('^result=0', exec_result):
                return 0
            else:
                return exec_result[self.res_idx:]
        else:
            return 1

    def create_fullvol_from_snap(self, vol_name, snap_name):
        cmd = '--op createFullVolumeFromSnap' + ' ' + '--volName' + ' ' + str(
            vol_name) + ' ' + '--snapName' + ' ' + str(snap_name)

        exec_result = self.start_execute_cmd(cmd, 0)
        if exec_result:
            if re.search('^result=0', exec_result):
                return 0
            else:
                result = int(exec_result[self.res_idx:])
        else:
            return 1
        if 50151005 == result:
            result = 0
        return result

    def create_linked_clone_volume(self, vol_name,
                                   vol_size, src_vol_name):
        tmp_snap_name = str(src_vol_name) + str(vol_name)

        ret_code = self.create_snapshot(tmp_snap_name, src_vol_name, 0)
        if 0 != ret_code:
            self.delete_snapshot(tmp_snap_name)
            return ret_code

        ret_code = self.create_volume_from_snap(
            vol_name, vol_size, tmp_snap_name)
        if 0 != ret_code:
            self.delete_snapshot(tmp_snap_name)
            self.delete_volume(vol_name)
            return ret_code

        self.delete_snapshot(tmp_snap_name)
        return 0

    def create_volume_from_volume(self, vol_name, vol_size, src_vol_name,
                                  is_encrypted=None, volume_cmkId=None,
                                  volume_auth_token=None, pool_id=0):
        tmp_snap_name = str(vol_name) + '_tmp_snap'

        ret_code = self.create_snapshot(tmp_snap_name, src_vol_name, 0)
        if 0 != ret_code:
            return ret_code

        ret_code = self.create_volume(vol_name, pool_id, vol_size, 0,
                                      is_encrypted, volume_cmkId,
                                      volume_auth_token)
        if 0 != ret_code:
            self.delete_snapshot(tmp_snap_name)
            return ret_code

        ret_code = self.create_fullvol_from_snap(vol_name, tmp_snap_name)
        if 0 != ret_code:
            self.delete_snapshot(tmp_snap_name)
            self.delete_volume(vol_name)
            return ret_code

        self.delete_snapshot(tmp_snap_name)
        return 0

    def create_clone_volume_from_volume(self, vol_name,
                                        vol_size, src_vol_name):
        tmp_snap_name = str(src_vol_name) + '_DT_clone_snap'

        ret_code = self.create_snapshot(tmp_snap_name, src_vol_name, 0)
        if 0 != ret_code:
            return ret_code

        ret_code = self.create_volume_from_snap(vol_name,
                                                vol_size,
                                                tmp_snap_name)
        if 0 != ret_code:
            self.delete_snapshot(tmp_snap_name)
            return ret_code
        self.delete_snapshot(tmp_snap_name)
        return 0

    def volume_info_analyze(self, vol_info):
        local_volume_info = volume_info
        if not vol_info:
            local_volume_info['result'] = 1
            return local_volume_info

        local_volume_info['result'] = 0

        vol_info_list = re.split(',', vol_info)
        for line in vol_info_list:
            line = line.replace('\n', '')
            params = re.split('=', line)
            param_key = params[0]
            if param_key in local_volume_info.keys():
                local_volume_info[param_key] = line[(len(param_key)+1):]
            else:
                LOG.debug("analyze key is no exist,key=%s", str(line))
        return local_volume_info

    def query_volume(self, vol_name):
        tmp_volume_info = volume_info
        cmd = '--op queryVolume' + ' ' + '--volName' + ' ' + vol_name

        exec_result = self.start_execute_cmd(cmd, 1)
        if exec_result:
            for line in exec_result:
                if re.search('^result=', line):
                    if not re.search('^result=0', line):
                        tmp_volume_info['result'] = int(line[self.res_idx:])
                        return tmp_volume_info
                    for vol_line in exec_result:
                        if re.search('^vol_name=' + vol_name, vol_line):
                            tmp_volume_info = self.volume_info_analyze(vol_line)
                            return tmp_volume_info

        tmp_volume_info['result'] = 1
        return tmp_volume_info

    def _check_qos_para(self, paras):
        if paras == '0' or paras == '':
            return 0
        else:
            return 1

    def volume_qos_info_analyze(self, qos_result_info):
        qos_para = qos_info.copy()
        para_num = 0

        for line in qos_result_info:
            if re.search('^result=', line):
                qos_para['result'] = int(line[len('result='):])
            elif re.search('^qosName:', line):
                qos_para['qos_name'] = line[len('qosName:'):]
            elif re.search('^createTime:', line):
                qos_para['create_time'] = line[len('createTime:'):]
            elif re.search('^IOPSPerGB:', line):
                qos_para['iops_perGB'] = line[len('IOPSPerGB:'):]
                para_num += self._check_qos_para(qos_para['iops_perGB'])
            elif re.search('^maxIOPS:', line):
                qos_para['max_iops'] = line[len('maxIOPS:'):]
                para_num += self._check_qos_para(qos_para['max_iops'])
            elif re.search('^burstIOPS:', line):
                qos_para['burst_iops'] = line[len('burstIOPS:'):]
                para_num += self._check_qos_para(qos_para['burst_iops'])
            elif re.search('^maxMBPS:', line):
                qos_para['max_mbps'] = line[len('maxMBPS:'):]
                para_num += self._check_qos_para(qos_para['max_mbps'])
            elif re.search('^burstMBPSPerTB:', line):
                qos_para['burst_mbps_perTB'] = line[len('burstMBPSPerTB:'):]
                para_num += self._check_qos_para(qos_para['burst_mbps_perTB'])
            elif re.search('^creditIOPS:', line):
                qos_para['credit_iops'] = line[len('creditIOPS:'):]
                para_num += self._check_qos_para(qos_para['credit_iops'])
            elif re.search('^minBaselineIOPS:', line):
                qos_para['min_base_line_iops'] = line[len('minBaselineIOPS:'):]
                para_num += self._check_qos_para(
                    qos_para['min_base_line_iops'])
            elif re.search('^minBaselineMBPS:', line):
                qos_para['min_base_line_mbps'] = line[len('minBaselineMBPS:'):]
                para_num += self._check_qos_para(
                    qos_para['min_base_line_mbps'])
            elif re.search('^readLimitIOPS:', line):
                qos_para['read_limit_iops'] = line[len('readLimitIOPS:'):]
                para_num += self._check_qos_para(qos_para['read_limit_iops'])
            elif re.search('^readLimitMBPS:', line):
                qos_para['read_limit_mbps'] = line[len('readLimitMBPS:'):]
                para_num += self._check_qos_para(qos_para['read_limit_mbps'])
            elif re.search('^writeLimitMBPS:', line):
                qos_para['write_limit_iops'] = line[len('writeLimitMBPS:'):]
                para_num += self._check_qos_para(qos_para['write_limit_iops'])
            elif re.search('^writeLimitIOPS:', line):
                qos_para['write_limit_mbps'] = line[len('writeLimitIOPS:'):]
                para_num += self._check_qos_para(qos_para['write_limit_mbps'])
            elif re.search('^MBPSPerTB:', line):
                qos_para['mbps_perTB'] = line[len('MBPSPerTB:'):]
                para_num += self._check_qos_para(qos_para['mbps_perTB'])
            else:
                LOG.debug("analyze key is no exist,key=%s", str(line))
        qos_para['para_num'] = para_num
        return qos_para

    def query_volume_qos(self, vol_name):
        vol_qos_info = qos_info.copy()
        cmd = '--op queryVolumeQoSInfo --volName ' + vol_name

        exec_result = self.start_execute_cmd(cmd, 1)
        if exec_result:
            for line in exec_result:
                if re.search('^result=', line):
                    if not re.search('^result=0', line):
                        vol_qos_info['result'] = line[self.res_idx:]
                        return vol_qos_info
                    else:
                        vol_qos_info = \
                            self.volume_qos_info_analyze(exec_result)
                        return vol_qos_info

        vol_qos_info['result'] = 1
        return vol_qos_info

    def delete_volume(self, vol_name):
        cmd = '--op deleteVolume' + ' ' + '--volName' + ' ' + vol_name

        exec_result = self.start_execute_cmd(cmd, 0)
        if exec_result:
            if re.search('^result=0', exec_result):
                return 0
            else:
                result = int(exec_result[self.res_idx:])
        else:
            return 1
        if 50150005 == result:
            result = 0
        return result

    def create_snapshot(self, snap_name, vol_name, smart_flag):
        cmd = '--op createSnapshot' + ' ' + '--volName' + ' ' + str(
            vol_name) + ' ' + '--snapName' + ' ' + str(
            snap_name) + ' ' + '--smartFlag' + ' ' + str(smart_flag)

        exec_result = self.start_execute_cmd(cmd, 0)
        if exec_result:
            if re.search('^result=0', exec_result):
                return 0
            else:
                return exec_result[self.res_idx:]
        else:
            return 1

    def snap_info_analyze(self, info):
        local_snap_info = snap_info.copy()
        if not info:
            local_snap_info['result'] = 1
            return local_snap_info

        local_snap_info['result'] = 0

        snap_info_list = re.split(',', info)
        for line in snap_info_list:
            line = line.replace('\n', '')
            params = re.split('=', line)
            param_key = params[0]
            if param_key in local_snap_info.keys():
                local_snap_info[param_key] = line[(len(param_key) + 1):]
            else:
                LOG.debug("analyze key is no exist,key=%s", str(line))
        return local_snap_info

    def query_snap(self, snap_name):
        tmp_snap_info = snap_info
        cmd = '--op querySnapshot' + ' ' + '--snapName' + ' ' + snap_name

        exec_result = self.start_execute_cmd(cmd, 1)
        if exec_result:
            for line in exec_result:
                if re.search('^result=', line):
                    if not re.search('^result=0', line):
                        tmp_snap_info['result'] = line[self.res_idx:]
                        return tmp_snap_info
                    for line in exec_result:
                        if re.search('^snap_name=' + snap_name, line):
                            tmp_snap_info = self.snap_info_analyze(line)
                            return tmp_snap_info

        tmp_snap_info['result'] = 1
        return tmp_snap_info

    def delete_snapshot(self, snap_name):
        cmd = '--op deleteSnapshot' + ' ' + '--snapName' + ' ' + snap_name

        exec_result = self.start_execute_cmd(cmd, 0)
        if exec_result:
            if re.search('^result=0', exec_result):
                return 0
            else:
                result = int(exec_result[self.res_idx:])
        else:
            return 1
        if 50150006 == result:
            LOG.info("snapshot %s is not exist" % snap_name)
            result = 0
        return result

    def pool_info_analyze(self, info):
        local_pool_info = pool_info.copy()
        if not info:
            local_pool_info['result'] = 1
            return local_pool_info

        local_pool_info['result'] = 0
        pool_info_list = re.split(',', info)
        for line in pool_info_list:
            line = line.replace('\n', '')
            params = re.split('=', line)
            param_key = params[0]
            if param_key in local_pool_info.keys():
                local_pool_info[param_key] = line[(len(param_key) + 1):]
            else:
                LOG.debug("analyze key is no exist,key=%s", str(line))
        return local_pool_info

    def query_pool_info(self, pool_id):
        tmp_pool_info = pool_info.copy()
        cmd = '--op queryPoolInfo' + ' ' + '--poolId' + ' ' + str(pool_id)
        LOG.debug("pool_id is %s", pool_id)
        exec_result = self.start_execute_cmd(cmd, 1)
        if exec_result:
            for line in exec_result:
                if re.search('^result=', line):
                    if not re.search('^result=0', line):
                        tmp_pool_info['result'] = line[self.res_idx:]
                        return tmp_pool_info
                    for line in exec_result:
                        if re.search('^pool_id=' + str(pool_id), line):
                            tmp_pool_info = self.pool_info_analyze(line)
                            return tmp_pool_info

        tmp_pool_info['result'] = 1
        return tmp_pool_info

    def query_pool_id_list(self, pool_id_list):
        pool_list = []
        for pool_id in pool_id_list:
            tmp_pool_info = self.query_pool_info(pool_id)
            if tmp_pool_info['result'] == 0:
                pool_list.append(tmp_pool_info)
            else:
                LOG.error("%(pool_id)s query fail, result is %(result)s",
                          {'pool_id': pool_id,
                           'result': tmp_pool_info['result']})

        return pool_list

    def query_pool_type(self, pool_type):
        pool_list = []
        result = 0
        cmd = '--op queryPoolType --poolType' + ' ' + pool_type
        LOG.debug("query poolType %s", pool_type)
        exec_result = self.start_execute_cmd(cmd, 1)
        if exec_result:
            for line in exec_result:
                line = line.replace('\n', '')
                if re.search('^result=', line):
                    if not re.search('^result=0', line):
                        result = int(line[self.res_idx:])
                        break
                    for one_line in exec_result:
                        if re.search('^pool_id=', one_line):
                            tmp_pool_info = self.pool_info_analyze(one_line)
                            pool_list.append(tmp_pool_info)
                    break
        return result, pool_list

    def query_dsware_version(self):
        ret_code = 2
        cmd = '--op getDSwareIdentifier'
        exec_result = self.start_execute_cmd(cmd, 0)
        if exec_result:
            # new version
            if re.search('^result=0', exec_result):
                ret_code = 0
            # old version
            elif re.search('^result=50500001', exec_result):
                ret_code = 1
            # failed
            else:
                ret_code = int(exec_result[self.res_idx:])
        return ret_code

    def create_snapshot_from_snap(self, src_snap_name, dst_snap_name,
                                  pool_id, fullCopyFlag, smartFlag):
        cmd = '--op duplicateSnapshot' + ' ' + '--snapNameSrc' + ' ' \
              + src_snap_name + ' ' + '--snapNameDst' + ' ' \
              + dst_snap_name + ' ' + '--poolId' + ' ' + str(pool_id) \
              + ' ' + '--fullCopyFlag' + ' ' + str(fullCopyFlag) + ' ' \
              + '--smartFlag' + ' ' + str(smartFlag)

        exec_result = self.start_execute_cmd(cmd, 0)
        if exec_result:
            if re.search('^result=0', exec_result):
                return 0
            else:
                return exec_result[self.res_idx:]
        else:
            return 1

    def query_volumes_from_snap(self, snap_name):
        result = 0
        volume_list = []
        cmd = '--op queryVolumeOfSnap' + ' ' + '--snapName' + ' ' + snap_name

        exec_result = self.start_execute_cmd(cmd, 1)
        if exec_result:
            for line in exec_result:
                line = line.replace('\n', '')
                if re.search('^result=', line):
                    if not re.search('^result=0', line):
                        result = int(line[self.res_idx:])
                        break
                    for one_line in exec_result:
                        if re.search('^vol_name=', one_line):
                            tmp_vol_info = self.volume_info_analyze(one_line)
                            volume_list.append(tmp_vol_info)
                    break

        # 51010013:no volume is created by this snap
        if 51010013 == result:
            result = 0
        return result, volume_list

    def rollback_snapshot(self, snap_name, vol_name):
        cmd = '--op rollbackSnapshot' + ' ' + '--snapName' + ' ' + str(
            snap_name) + ' ' + '--volName' + ' ' + str(vol_name)

        exec_result = self.start_execute_cmd(cmd, 0)
        if exec_result:
            if re.search('^result=0', exec_result):
                return 0
            else:
                result = int(exec_result[self.res_idx:])
        else:
            return 1
        if 50153010 == result:
            LOG.info("return success result is %s" % exec_result)
            result = 0
        return result

    def active_snapshots(self, volume_list, snapshot_list):
        cmd = '--op activeSnapshots' + ' ' + '--snapNameList' + ' ' + str(
            snapshot_list) + ' ' + '--volNameList' + ' ' + str(volume_list)

        exec_result = self.start_execute_cmd(cmd, 0)
        if exec_result:
            if re.search('^result=0', exec_result):
                return 0
            else:
                return exec_result[self.res_idx:]
        else:
            return 1

    def _deal_common_exec_result(self, exec_result):
        if exec_result:
            if re.search('^result=0', exec_result):
                return 0
            else:
                return int(exec_result[self.res_idx:])
        else:
            return 1

    def _get_qos_cmd_paras(self, qos):
        cmd = ' --qosName ' + qos['qos_name']
        if qos.get('max_iops') != '':
            cmd = cmd + ' --maxIOPS ' + qos['max_iops']
        if qos.get('max_mbps') != '':
            cmd = cmd + ' --maxMBPS ' + qos['max_mbps']
        if qos.get('burst_iops') != '':
            cmd = cmd + ' --burstIOPS ' + qos['burst_iops']
        if qos.get('credit_iops') != '':
            cmd = cmd + ' --creditIOPS ' + qos['credit_iops']
        if qos.get('iops_perGB') != '':
            cmd = cmd + ' --IOPSPerGB ' + qos['iops_perGB']
        if qos.get('read_limit_iops') != '':
            cmd = cmd + ' --readLimitIOPS ' + qos['read_limit_iops']
        if qos.get('read_limit_mbps') != '':
            cmd = cmd + ' --readLimitMBPS ' + qos['read_limit_mbps']
        if qos.get('write_limit_iops') != '':
            cmd = cmd + ' --writeLimitIOPS ' + qos['write_limit_iops']
        if qos.get('write_limit_mbps') != '':
            cmd = cmd + ' --writeLimitMBPS ' + qos['write_limit_mbps']
        if qos.get('burst_mbps_perTB') != '':
            cmd = cmd + ' --burstMBPSPerTB ' + qos['burst_mbps_perTB']
        if qos.get('mbps_perTB') != '':
            cmd = cmd + ' --MBPSPerTB ' + qos['mbps_perTB']
        if qos.get('min_base_line_mbps') != '':
            cmd = cmd + ' --minBaselineMBPS ' + qos['min_base_line_mbps']
        if qos.get('min_base_line_iops') != '':
            cmd = cmd + ' --minBaselineIOPS ' + qos['min_base_line_iops']
        return cmd

    def create_qos(self, qos):
        cmd = self._get_qos_cmd_paras(qos)
        cmd = '--op createQoS' + cmd
        exec_result = self.start_execute_cmd(cmd, 0)
        return self._deal_common_exec_result(exec_result)

    def delete_qos(self, qos_name):
        cmd = '--op deleteQoS --qosName ' + str(qos_name)
        exec_result = self.start_execute_cmd(cmd, 0)
        return self._deal_common_exec_result(exec_result)

    def update_qos(self, qos):
        cmd = self._get_qos_cmd_paras(qos)
        cmd = '--op setQoS' + cmd
        exec_result = self.start_execute_cmd(cmd, 0)
        return self._deal_common_exec_result(exec_result)

    def associate_qos_with_volume(self, qos_name, volume_name):
        cmd = '--op associateQoSWithVolume --qosName ' + str(qos_name) \
              + ' --volName ' + str(volume_name)
        exec_result = self.start_execute_cmd(cmd, 0)
        return self._deal_common_exec_result(exec_result)

    def disassociate_qos_with_volume(self, qos_name, volume_name):
        cmd = '--op disassociateQoSWithVolume --qosName ' + str(qos_name) \
              + ' --volName ' + str(volume_name)
        exec_result = self.start_execute_cmd(cmd, 0)
        return self._deal_common_exec_result(exec_result)

    def create_host(self, host_name):
        cmd = '--op createHost' + ' ' + '--hostName' + ' ' + str(host_name)
        exec_result = self.start_execute_cmd(cmd, 0)
        return self._deal_common_exec_result(exec_result)

    def delete_host(self, host_name):
        cmd = '--op deleteHost' + ' ' + '--hostName' + ' ' + str(host_name)
        exec_result = self.start_execute_cmd(cmd, 0)
        return self._deal_common_exec_result(exec_result)

    def create_port(self, port_name):
        cmd = '--op createPort' + ' ' + '--portName' + ' ' + str(port_name)
        exec_result = self.start_execute_cmd(cmd, 0)
        return self._deal_common_exec_result(exec_result)

    def delete_port(self, port_name):
        cmd = '--op deletePort' + ' ' + '--portName' + ' ' + str(port_name)
        exec_result = self.start_execute_cmd(cmd, 0)
        return self._deal_common_exec_result(exec_result)

    def add_port_to_host(self, host_name, port_name):
        cmd = '--op addPortToHost' + ' ' + '--portName' + ' ' + str(port_name) \
              + ' ' + '--hostName' + ' ' + str(host_name)
        exec_result = self.start_execute_cmd(cmd, 0)
        return self._deal_common_exec_result(exec_result)

    def del_port_from_host(self, host_name, port_name):
        cmd = '--op delPortFromHost' + ' ' + '--portName' + ' ' + \
              str(port_name) + ' ' + '--hostName' + ' ' + str(host_name)
        exec_result = self.start_execute_cmd(cmd, 0)
        return self._deal_common_exec_result(exec_result)

    def add_lun_to_host(self, host_name, volume_name):
        cmd = '--op addLunToHost' + ' ' + '--hostName' + ' ' + str(host_name) \
              + ' ' + '--volName' + ' ' + str(volume_name)
        exec_result = self.start_execute_cmd(cmd, 0)
        return self._deal_common_exec_result(exec_result)

    def del_lun_from_host(self, host_name, volume_name):
        cmd = '--op delLunFromHost' + ' ' + '--hostName' + ' ' + \
              str(host_name) + ' ' + '--volName' + ' ' + str(volume_name)
        exec_result = self.start_execute_cmd(cmd, 0)
        return self._deal_common_exec_result(exec_result)

    def query_host_lun_info(self, host_name):
        cmd = '--op queryHostLunInfo' + ' ' + '--hostName' \
              + ' ' + str(host_name)
        exec_result = self.start_execute_cmd(cmd, 1)
        if exec_result:
            return exec_result
        else:
            return 1

    def create_host_group(self, host_group_name):
        cmd = '--op createHostGroup' + ' ' + '--hostGroupName' \
              + ' ' + str(host_group_name)
        exec_result = self.start_execute_cmd(cmd, 0)
        return self._deal_common_exec_result(exec_result)

    def delete_host_group(self, host_group_name):
        cmd = '--op deleteHostGroup' + ' ' + '--hostGroupName' \
              + ' ' + str(host_group_name)
        exec_result = self.start_execute_cmd(cmd, 0)
        return self._deal_common_exec_result(exec_result)

    def add_host_to_hostgroup(self, host_name, hostgp_name):
        cmd = '--op addHostToHostGroup' + ' ' + '--hostName' + ' ' + \
              str(host_name) + ' ' + '--hostGroupName' + ' ' + str(hostgp_name)
        exec_result = self.start_execute_cmd(cmd, 0)
        return self._deal_common_exec_result(exec_result)

    def del_host_from_hostgroup(self, host_name, hostgp_name):
        cmd = '--op delHostFromHostGroup' + ' ' + '--hostName' + ' ' + \
              str(host_name) + ' ' + '--hostGroupName' + ' ' + str(hostgp_name)
        exec_result = self.start_execute_cmd(cmd, 0)
        return self._deal_common_exec_result(exec_result)

    def add_volume_to_hostgroup(self, volume_name, hostgp_name):
        cmd = '--op addVolumeToHostGroup' + ' ' + '--hostGroupName' + ' ' \
              + str(hostgp_name) + ' ' + '--volName' + ' ' + str(volume_name)
        exec_result = self.start_execute_cmd(cmd, 0)
        return self._deal_common_exec_result(exec_result)

    def del_volume_from_hostgroup(self, volume_name, hostgp_name):
        cmd = '--op delVolumeFromHostGroup' + ' ' + '--hostGroupName' + ' ' \
              + str(hostgp_name) + ' ' + '--volName' + ' ' + str(volume_name)
        exec_result = self.start_execute_cmd(cmd, 0)
        return self._deal_common_exec_result(exec_result)

    def query_lun_from_hostgroup(self, hostgroup_name):
        cmd = '--op queryLunFromHostGroup' + ' ' + '--hostGroupName' \
              + ' ' + str(hostgroup_name)
        exec_result = self.start_execute_cmd(cmd, 1)
        if exec_result:
            return exec_result
        else:
            return 1

    def query_portal_info(self, port_name):
        portal_list = []
        cmd = '--op queryIscsiPortalInfo' + ' ' + '--portName' \
              + ' ' + str(port_name)
        exec_result = self.start_execute_cmd(cmd, 1)
        for line in exec_result:
            if "," in line:
                portal_list.append(line)
        return portal_list

    def attach_volume(self, volume_name, target_ip):
        volume_attach_result = volume_attach_info
        cmd = '--op attachVolume' + ' ' + '--volName' \
              + ' ' + str(volume_name)
        exec_result = self.start_execute_cmd_with_ip(cmd, target_ip)
        if not exec_result:
            volume_attach_result['result'] = 1
            return volume_attach_result
        for line in exec_result:
            if not re.search('^result=', line):
                continue
            volume_attach_result['result'] = int(line[self.res_idx:])
            if not re.search('^result=0', line):
                return volume_attach_result
            for result_line in exec_result:
                if re.search(', devName=', result_line):
                    volume_attach_result['devName'] = result_line[len(
                        'DSwareAttachResult [result=0, devName='):]
                    return volume_attach_result
        volume_attach_result['result'] = 1
        return volume_attach_result

    def detach_volume(self, volume_name, target_ip):
        volume_detach_result = volume_attach_info
        cmd = '--op detachVolume' + ' ' + '--volName' \
              + ' ' + str(volume_name)
        exec_result = self.start_execute_cmd_with_ip(cmd, target_ip)
        result = 1
        if exec_result:
            for line in exec_result:
                if re.search('^result=', line):
                    result = int(line[self.res_idx:])
        else:
            LOG.error("fsc_cli not found result %s" % exec_result)
            result = 1
        if 50151601 == result:
            result = 0
        volume_detach_result['result'] = result
        return volume_detach_result

    def detach_volume_by_ip(self, volume_name, detach_ip):
        volume_detach_result = volume_attach_info
        cmd = '--op detachVolumeByIp' + ' ' + '--volName' \
              + ' ' + str(volume_name) + ' ' + '--detachIp' + ' ' + detach_ip
        exec_result = self.start_execute_cmd_to_all(cmd)
        error_code = self._deal_common_exec_result(exec_result)
        volume_detach_result['result'] = error_code
        return volume_detach_result

    def _query_snapshot_of_volume(self, vol_name):
        result = 0
        snapshot_list = []
        cmd = '--op querySnapOfVolume' + ' ' + '--volName' + ' ' + vol_name

        exec_result = self.start_execute_cmd(cmd, 1)
        if exec_result:
            for line in exec_result:
                line = line.replace('\n', '')
                if re.search('^result=', line) and \
                        not re.search('^result=0', line):
                    result = int(line[self.res_idx:])
                    break
        # 51010014:no snap is created by this volume
        if 51010014 == result or 0 == result:
            result = 0
            snapshot_list = self._get_snapshot_list_from_result(exec_result)
        return result, snapshot_list

    def _get_snapshot_list_from_result(self, exec_result):
        snapshot_list = []
        if not exec_result:
            return snapshot_list

        for one_line in exec_result:
            if re.search('^snap_name=', one_line):
                tmp_snap_info = self.snap_info_analyze(one_line)
                snapshot_list.append(tmp_snap_info)
        return snapshot_list

    def _get_snapshot_from_result(self, snapshot_list, snapshot_key,
                                  snapshot_name):
        for snapshot_info in snapshot_list:
            LOG.debug("the snapshot_info %s", snapshot_info.get(snapshot_key))
            LOG.debug("the snapshot_name %s", snapshot_name)
            if snapshot_info.get(snapshot_key) == snapshot_name:
                return snapshot_info

        return {}

    def query_snapshot_of_volume(self, vol_name, snapshot_name):
        result, snapshot_list = self._query_snapshot_of_volume(vol_name)
        if result == 0 and snapshot_list:
            snapshot_info = self._get_snapshot_from_result(
                snapshot_list, 'snap_name', snapshot_name)
            if snapshot_info:
                return snapshot_info

        return {}
