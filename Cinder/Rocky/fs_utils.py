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

import datetime
import pytz
import time

from oslo_log import log as logging

from cinder import context
from cinder import exception
from cinder.i18n import _
from cinder import objects
from cinder.volume.drivers.fusionstorage import constants
from cinder.volume import qos_specs
from cinder.volume import volume_types


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


def _get_target_portal(port_list):
    for port in port_list:
        if port.get("iscsiStatus") == "active":
            return port.get("iscsiPortal"), port.get('targetName')
    return None, None


def get_target_portal(client, target_ip):
    tgt_portal = client.get_target_port(target_ip)
    for node_portal in tgt_portal:
        if node_portal.get("nodeMgrIp") == target_ip:
            port_list = node_portal.get("iscsiPortalList", [])
            return _get_target_portal(port_list)


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


def get_volume_params(volume, client):
    volume_type = _get_volume_type(volume)
    return get_volume_type_params(volume_type, client)


def _get_volume_type(volume):
    if volume.volume_type:
        return volume.volume_type
    if volume.volume_type_id:
        return volume_types.get_volume_type(None, volume.volume_type_id)


def get_volume_type_params(volume_type, client):
    vol_params = {}

    if isinstance(volume_type, dict) and volume_type.get('qos_specs_id'):
        vol_params['qos'] = _get_qos_specs(volume_type['qos_specs_id'], client)
    elif isinstance(volume_type, objects.VolumeType
                    ) and volume_type.qos_specs_id:
        vol_params['qos'] = _get_qos_specs(volume_type.qos_specs_id, client)

    LOG.info('volume opts %s.', vol_params)
    return vol_params


def _get_trigger_qos(qos, client):
    if qos.get(constants.QOS_SCHEDULER_KEYS[0]):
        if client.get_fsm_version() >= constants.QOS_SUPPORT_SCHEDULE_VERSION:
            qos = _check_and_convert_qos(qos, client)
        else:
            msg = _('FusionStorage Version is not suitable for QoS: %s') % qos
            LOG.error(msg)
            raise exception.InvalidInput(reason=msg)
    return qos


def _is_qos_specs_valid(specs):
    if specs is None:
        return False

    if specs.get('consumer') == 'front-end':
        return False
    return True


def _raise_qos_not_set(qos):
    for item in constants.QOS_MUST_SET:
        if item not in qos:
            msg = _('%s must be set for QoS: %s') % (
                constants.QOS_MUST_SET, qos)
            LOG.error(msg)
            raise exception.InvalidInput(reason=msg)


def _raise_qos_is_invalid(qos_key):
    if qos_key not in constants.QOS_KEYS + constants.QOS_SCHEDULER_KEYS:
        msg = _('QoS key %s is not valid.') % qos_key
        LOG.error(msg)
        raise exception.InvalidInput(reason=msg)


def _set_qos(qos, qos_key, qos_value):
    if qos_key in constants.QOS_KEYS:
        if int(qos_value) <= 0:
            msg = _('QoS value for %s must > 0.') % qos_key
            LOG.error(msg)
            raise exception.InvalidInput(reason=msg)

        qos[qos_key] = int(qos_value)
    elif qos_key in constants.QOS_SCHEDULER_KEYS:
        qos[qos_key] = qos_value.strip()
    return qos


def _get_qos_specs(qos_specs_id, client):
    ctxt = context.get_admin_context()
    specs = qos_specs.get_qos_specs(ctxt, qos_specs_id)
    if not _is_qos_specs_valid(specs):
        return {}

    kvs = specs.get('specs', {})
    LOG.info('The QoS specs is: %s.', kvs)

    qos = dict()
    for k, v in kvs.items():
        _raise_qos_is_invalid(k)
        qos = _set_qos(qos, k, v)

    _raise_qos_not_set(qos)
    qos = _get_trigger_qos(qos, client)

    return qos


def _deal_date_increase_or_decrease(is_date_decrease, is_date_increase, qos):
    if is_date_decrease:
        config_date_sec = qos[constants.QOS_SCHEDULER_KEYS[1]]
        qos[constants.QOS_SCHEDULER_KEYS[1]] = (config_date_sec -
                                                constants.SECONDS_OF_DAY)

    if is_date_increase:
        config_date_sec = qos[constants.QOS_SCHEDULER_KEYS[1]]
        qos[constants.QOS_SCHEDULER_KEYS[1]] = (config_date_sec +
                                                constants.SECONDS_OF_DAY)
    return qos


def _check_default_scheduler(qos, is_default_scheduler, configed_none_default):
    if is_default_scheduler and configed_none_default:
        msg = (_("The default scheduler: %(type)s is not allowed to config "
                 "other scheduler policy")
               % {"type": qos[constants.QOS_SCHEDULER_KEYS[0]]})
        LOG.error(msg)
        raise exception.InvalidInput(msg)


def _check_week_scheduler(qos, configed_week_scheduler, configed_none_default):
    if configed_week_scheduler and (
            configed_none_default != len(constants.QOS_SCHEDULER_KEYS) - 1):
        msg = (_("The week scheduler type %(type)s params number are "
                 "incorrect.")
               % {"type": qos[constants.QOS_SCHEDULER_KEYS[0]]})
        LOG.error(msg)
        raise exception.InvalidInput(msg)


def _check_scheduler_count(qos, is_default_scheduler, configed_week_scheduler,
                           configed_none_default):
    if (not is_default_scheduler and not configed_week_scheduler and
            configed_none_default != len(constants.QOS_SCHEDULER_KEYS) - 2):
        msg = (_("The scheduler type %(type)s params number are incorrect.")
               % {"type": qos[constants.QOS_SCHEDULER_KEYS[0]]})
        LOG.error(msg)
        raise exception.InvalidInput(msg)


def _check_and_convert_qos(qos, client):
    configed_none_default = 0
    sys_loc_time = _get_sys_time(client)
    sys_loc_time = time.strptime(datetime.datetime.now(sys_loc_time).strftime(
        "%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")

    (qos, is_default_scheduler,
     configed_week_scheduler) = _convert_schedule_type(qos)

    qos, configed_none_default = _convert_start_date(
        qos, sys_loc_time, configed_none_default)

    (qos, configed_none_default,
     is_date_decrease, is_date_increase) = _convert_start_time(
        qos, client, sys_loc_time, configed_none_default)

    qos, configed_none_default = _convert_duration_time(
        qos, configed_none_default)

    qos, configed_none_default = _convert_day_of_week(
        qos, configed_none_default)

    _check_default_scheduler(qos, is_default_scheduler, configed_none_default)
    _check_week_scheduler(qos, configed_week_scheduler, configed_none_default)
    _check_scheduler_count(qos, is_default_scheduler, configed_week_scheduler,
                           configed_none_default)

    return _deal_date_increase_or_decrease(
        is_date_decrease, is_date_increase, qos)


def _get_sys_time(client):
    time_zone = client.get_system_time_zone()
    try:
        sys_loc_time = pytz.timezone(time_zone)
    except Exception as err:
        LOG.warning("Time zone %(zone)s does not exist in the operating "
                    "system, reason: %(err)s"
                    % {"zone": time_zone, "err": err})
        sys_loc_time = pytz.timezone(constants.TIMEZONE[time_zone])
    return sys_loc_time


def _deal_dst_time(time_config, cur_time):
    LOG.info("Current system time is %(cur)s.", {"cur": cur_time})
    use_dst = int(time_config.get("use_dst", 0))
    # Current time is or not dst time
    cur_is_in_dst = False
    if use_dst:
        start_time = time_config["dst_begin_date"]
        end_time = time_config["dst_end_date"]
        if (end_time >= cur_time >= start_time or
                cur_time <= end_time < start_time or
                end_time < start_time <= cur_time):
            cur_is_in_dst = True

    LOG.info("Current date in DST: %(cur)s.", {"cur": cur_is_in_dst})
    return cur_is_in_dst


def _get_qos_time_params_east_zone(time_zone, config_sec,
                                   cur_date_in_dst_time):
    is_date_decrease = False
    if cur_date_in_dst_time:
        time_zone += constants.SECONDS_OF_HOUR

    if config_sec >= time_zone:
        qos_time_params = int(config_sec - time_zone)
    else:
        qos_time_params = int(config_sec + (constants.SECONDS_OF_DAY
                                            - time_zone))
        is_date_decrease = True
    return qos_time_params, is_date_decrease


def _get_qos_time_params_west_zone(time_zone, config_sec,
                                   cur_date_in_dst_time):
    is_date_increase = False
    if cur_date_in_dst_time:
        time_zone -= constants.SECONDS_OF_HOUR
    if config_sec + time_zone < constants.SECONDS_OF_DAY:
        qos_time_params = int(config_sec + time_zone)
    else:
        qos_time_params = int(config_sec + time_zone -
                              constants.SECONDS_OF_DAY)
        is_date_increase = True
    return qos_time_params, is_date_increase


def _get_qos_time_params(zone_flag, time_zone, config_sec,
                         cur_date_in_dst_time):
    LOG.info("time_zone is: %(time_zone)s, zone flag is: %(zone)s "
             "config time_seconds is: %(config)s",
             {"time_zone": time_zone, "zone": zone_flag,
              "config": config_sec})
    is_date_increase = False
    is_date_decrease = False
    if zone_flag:
        qos_time_params, is_date_decrease = _get_qos_time_params_east_zone(
            time_zone, config_sec, cur_date_in_dst_time)
    else:
        qos_time_params, is_date_increase = _get_qos_time_params_west_zone(
            time_zone, config_sec, cur_date_in_dst_time)
    LOG.info("qos time is: %(time)s, is_date_decrease is %(decrease)s, "
             "is_date_increase is %(crease)s" % {"time": qos_time_params,
                                               "decrease": is_date_decrease,
                                               "crease": is_date_increase})
    return qos_time_params, is_date_decrease, is_date_increase


def _convert_schedule_type(qos):
    is_default_scheduler = True
    configed_week_scheduler = False
    schedule_type = constants.QOS_SCHEDULER_KEYS[0]
    if qos.get(schedule_type):
        # Distinguish type
        if qos[schedule_type] != constants.QOS_SCHEDULER_DEFAULT_TYPE:
            is_default_scheduler = False
        if qos[schedule_type] == constants.QOS_SCHEDULER_WEEK_TYPE:
            configed_week_scheduler = True
        qos[schedule_type] = int(qos[schedule_type])

    return qos, is_default_scheduler, configed_week_scheduler


def _get_diff_time(time_config):
    time_zone = time_config.get("time_zone")
    if not time_zone:
        msg = _("The time zone info %s is invalid.") % time_zone
        LOG.info(msg)
        raise exception.InvalidInput(msg)

    zone_flag, time_zone = ((False, time_zone.split("-")[1])
                            if "-" in time_zone
                            else (True, time_zone.split("+")[1]))
    time_zone = time.strptime(time_zone, '%H:%M')
    diff_time = datetime.timedelta(hours=time_zone.tm_hour,
                                   minutes=time_zone.tm_min).seconds
    return zone_flag, diff_time


def _convert_start_date(qos, sys_loc_time, configed_none_default):
    start_date = constants.QOS_SCHEDULER_KEYS[1]
    sys_date_time = time.strftime("%Y-%m-%d", sys_loc_time)
    if qos.get(start_date):
        # Convert the config date to timestamp
        cur_date = time.mktime(time.strptime(
            sys_date_time, '%Y-%m-%d')) - time.timezone
        try:
            config_date = time.mktime(time.strptime(
                qos[start_date], '%Y-%m-%d')) - time.timezone
        except Exception as err:
            msg = (_("The start date %(date)s is illegal. Reason: %(err)s")
                   % {"date": qos[start_date], "err": err})
            LOG.error(msg)
            raise exception.InvalidInput(msg)

        if config_date < cur_date:
            msg = (_("The start date %(date)s is earlier than current "
                     "time") % {"date": qos[start_date]})
            LOG.error(msg)
            raise exception.InvalidInput(msg)
        qos[start_date] = int(config_date)
        configed_none_default += 1
    return qos, configed_none_default


def _convert_start_time(qos, client, sys_loc_time, configed_none_default):
    start_date = constants.QOS_SCHEDULER_KEYS[1]
    start_time = constants.QOS_SCHEDULER_KEYS[2]
    is_date_increase = False
    is_date_decrease = False
    sys_dst_time = time.strftime("%m-%d %H:%M:%S", sys_loc_time)
    if qos.get(start_time):
        if qos.get(start_date) is None:
            msg = (_("The start date %(date)s is not config.")
                   % {"date": qos.get(start_date)})
            LOG.error(msg)
            raise exception.InvalidInput(msg)
        # Convert the config time to green time
        try:
            config_time = time.strptime(qos[start_time], "%H:%M")
        except Exception as err:
            msg = (_("The start time %(time)s is illegal. Reason: %(err)s")
                   % {"time": qos[start_time], "err": err})
            LOG.error(msg)
            raise exception.InvalidInput(msg)
        config_sec = datetime.timedelta(
            hours=config_time.tm_hour, minutes=config_time.tm_min).seconds

        time_config = client.get_time_config()

        cur_date_in_dst_time = _deal_dst_time(
            time_config, sys_dst_time)

        LOG.info("System time is: %s", sys_loc_time)
        zone_flag, time_zone = _get_diff_time(time_config)

        (qos_time_params, is_date_decrease,
         is_date_increase) = _get_qos_time_params(
            zone_flag, time_zone, config_sec,
            cur_date_in_dst_time)

        qos[start_time] = qos_time_params
        configed_none_default += 1
    return qos, configed_none_default, is_date_decrease, is_date_increase


def _convert_duration_time(qos, configed_none_default):
    duration_time = constants.QOS_SCHEDULER_KEYS[3]
    if qos.get(duration_time):
        # Convert the config duration time to seconds
        if qos[duration_time] == "24:00":
            config_duration_sec = constants.SECONDS_OF_DAY
        else:
            try:
                config_duration_time = time.strptime(
                    qos[duration_time], "%H:%M")
            except Exception as err:
                msg = (_("The duration time %(time)s is illegal. "
                         "Reason: %(err)s")
                       % {"time": qos[duration_time], "err": err})
                LOG.error(msg)
                raise exception.InvalidInput(msg)

            config_duration_sec = datetime.timedelta(
                hours=config_duration_time.tm_hour,
                minutes=config_duration_time.tm_min).seconds
        qos[duration_time] = int(config_duration_sec)
        configed_none_default += 1
    return qos, configed_none_default


def _is_config_weekday_valid(config_days_list, config_days):
    for config in config_days_list:
        if config not in constants.WEEK_DAYS:
            msg = (_("The week day %s is illegal.") % config_days)
            LOG.error(msg)
            raise exception.InvalidInput(msg)


def _convert_day_of_week(qos, configed_none_default):
    day_of_week = constants.QOS_SCHEDULER_KEYS[4]
    if qos.get(day_of_week):
        # Convert the week days
        config_days = 0
        config_days_list = qos[day_of_week].split()
        _is_config_weekday_valid(config_days_list, qos[day_of_week])

        for index in range(len(constants.WEEK_DAYS)):
            if constants.WEEK_DAYS[index] in config_days_list:
                config_days += pow(2, index)
        qos[day_of_week] = int(config_days)
        configed_none_default += 1
    return qos, configed_none_default


def get_volume_specs(client, vol_name):
    vol_info = {}
    qos_info = {}
    vol_qos = client.get_qos_by_vol_name(vol_name)
    for key, value in vol_qos.get("qosSpecInfo", {}).items():
        if (key in (constants.QOS_KEYS + constants.QOS_SCHEDULER_KEYS) and
                int(value)):
            qos_info[key] = int(value)
    vol_info['qos'] = qos_info
    return vol_info
