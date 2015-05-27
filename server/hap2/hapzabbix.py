#! /usr/bin/env python
# coding: UTF-8
"""
  Copyright (C) 2015 Project Hatohol

  This file is part of Hatohol.

  Hatohol is free software: you can redistribute it and/or modify
  it under the terms of the GNU Lesser General Public License, version 3
  as published by the Free Software Foundation.

  Hatohol is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
  GNU Lesser General Public License for more details.

  You should have received a copy of the GNU Lesser General Public
  License along with Hatohol. If not, see
  <http://www.gnu.org/licenses/>.
"""

import daemon
import json
import multiprocessing
import Queue
import argparse
import time

from haplib import HAPUtils, HAPBaseSender,HAPBaseReceiver, HAPBaseMainPlugin,\
                   ArmInfo, ERROR_DICT, SERVER_PROCEDURES
import zabbixapi

class PreviousHostsInfo:
    def __init__(self):
        self.hosts = list()
        self.host_groups = list()
        self.host_group_membeship = list()


class HAPZabbixMainPlugin(HAPBaseMainPlugin):
    def __init__(self, host, port, vhost, queue_name, user_name,
                 user_password, main_request_queue, main_response_queue,
                 ms_info=None):
        HAPBaseMainPlugin.__init__(self, main_request_queue)
        self.sender = HAPZabbixSender(host, port, vhost, queue_name, user_name,
                                      user_password, main_response_queue,
                                      ms_info)

    def hap_fetch_items(self, params, request_id):
        self.sender.send_response_to_queue("SUCCESS", request_id)
        self.sender.put_items(params["hostId"], params["fetchId"])

    def hap_fetch_history(self, params, request_id):
        self.sender.send_response_to_queue("SUCCESS", request_id)
        self.sender.put_history(params["itemId"], params["fetchId"])

    def hap_fetch_triggers(self, params, request_id):
        self.sender.send_response_to_queue("SUCCESS", request_id)
        self.sender.update_triggers(params["lastChangeTime"], params["hostId"],
                                    params["fetchId"])

    def hap_fetch_events(self, params, request_id):
        self.sender.send_response_to_queue("SUCCESS", request_id)
        self.sender.update_events(params["lastInfo"], params["count"],
                                  params["direction"], params["fetchId"])


class HAPZabbixSender(HAPBaseSender):
    def __init__(self, host, port, vhost, queue_name, user_name,
                 user_password, sender_queue, ms_info=None):
        HAPBaseSender.__init__(self, host, port, vhost, queue_name, user_name,
                               user_password, sender_queue, ms_info)
        self.api = zabbixapi.ZabbixAPI(self.ms_info)
        self.previous_hosts_info = PreviousHostsInfo()
        self.trigger_last_info = None
        self.event_last_info = None

    def put_items(self, host_id=None, fetch_id=None):
        params = {"items": self.api.get_items(host_id)}
        if fetch_id is not None:
            params["fetchId"] = fetch_id

        request_id = HAPUtils.get_and_save_request_id(self.requested_ids)
        self.send_request_to_queue("putItems", params, request_id)

        self.get_response_and_check_id(request_id)

    def put_history(self, item_id, fetch_id):
        params = {"itemId": item_id,
                  "histories": self.api.get_history(item_id),
                  "fetchId": fetch_id}

        request_id = HAPUtils.get_and_save_request_id(self.requested_ids)
        self.send_request_to_queue("putHistory", params, request_id)

        self.get_response_and_check_id(request_id)

    def update_hosts_and_host_group_membership(self):
        hosts, hg_membership = self.api.get_hosts()

        hosts.sort()
        if self.previous_hosts_info.hosts != hosts:
            hosts_params = {"updateType": "ALL", "hosts": hosts}
            request_id = HAPUtils.get_and_save_request_id(self.requested_ids)
            self.send_request_to_queue("updateHosts", hosts_params, request_id)
            self.get_response_and_check_id(request_id)
            self.previous_hosts_info.hosts = hosts

        hg_membership.sort()
        if self.previous_hosts_info.host_group_membership != hg_membership:
            hg_membership_params = {"updateType": "ALL",
                                    "hostGroupMembership": hg_membership}
            request_id = HAPUtils.get_and_save_request_id(self.requested_ids)
            self.send_request_to_queue("updateHostGroupMembership",
                                       hg_membership_params, request_id)
            self.get_response_and_check_id(request_id)
            self.previous_hosts_info.host_group_membership = hg_membership

    def update_host_groups(self):
        host_groups = api.get_host_groups()
        host_groups.sort()
        if self.previous_host_groups != host_groups:
            hosts_params = {"updateType": "ALL", "hostGroups": host_groups}
            request_id = HAPUtils.get_and_save_request_id(self.requested_ids)
            self.send_request_to_queue("updateHostGroups", params, request_id)
            self.get_response_and_check_id(request_id)
            self.previous_host_groups = host_groups

    def update_triggers(self, host_id=None, fetchId=None):
        if self.trigger_last_info is None:
            self.trigger_last_info = self.get_last_info("trigger")

        triggers = self.api.get_triggers(self.trigger_last_info, host_id)
        self.trigger_last_info =											\
            HAPUtils.find_last_info_from_dict_array(triggers, "lastChangeTime")

        params = {"triggers": triggers, "updateType": "UPDATED",
                  "lastInfo": self.trigger_last_info}

        if fetch_id is not None:
            params["fetchId"] = fetch_id
            params["updateType"] = "ALL"

        request_id = HAPUtils.get_and_save_request_id(self.requested_ids)
        self.send_request_to_queue("updateTriggers", params, request_id)
        self.get_response_and_check_id(request_id)

    def update_events(self, last_info=None, count=None, direction="ASC",
                      fetch_id=None):
        if last_info is None:
            last_info = self.get_last_info("event")

        if direction == "ASC":
            event_id_from = last_info
            event_id_till = None
            if count is not None:
                event_id_till = event_id_from + count
        # The following elif sentence is used from only fetchEvents
        elif direction == "DESC":
            event_id_till = last_info
            event_id_from = event_id_till - count

        events = self.api.get_events(event_id_from, event_id_till)

        count = len(events) / 1000 + 1
        for num in range(0, count):
            start = num * 1000
            send_events = events[start: start + 1000]
            last_info = HAPUtils.find_last_info_from_dict_array(send_events,
                                                              "eventId")
            params = {"events": send_events, "lastInfo": last_info,
                      "updateType": "UPDATE"}

            if fetch_id is not None:
                params["fetchId"] = fetch_id

            if num < count - 1:
                params["mayMoreFlag"] = True

            request_id = HAPUtils.get_and_save_request_id(self.requested_ids)
            self.send_request_to_queue("updateTriggers", params, request_id)
            self.get_response_and_check_id(request_id)

        self.event_last_info = last_info


class HAPZabbixPoller:
    def __init__(self, host, port, vhost, queue_name, user_name,
                 user_password, poller_queue):
        self.sender = HAPZabbixSender(host,
                                      port,
                                      vhost,
                                      queue_name,
                                      user_name,
                                      user_password,
                                      poller_queue)

    def update_lump(self):
        self.sender.put_items()
        self.sender.update_hosts_and_host_group_membership()
        self.sender.update_host_groups()
        self.sender.update_triggers()
        self.sender.update_events()

    def poll(self):
        arm_info = ArmInfo()
        while True:
            sleep_time = self.sender.ms_info.polling_interval_sec
            try:
                self.update_lump()
                arm_info.last_status = "OK"
                arm_info.failure_reason = ""
                arm_info.last_success_time = HAPUtils.get_current_hatohol_time()
                arm_info.num_success += 1
            except:
                sleep_time = self.sender.ms_info.retry_interval_sec
                arm_info.last_status = "NG"
                #ToDo Think about how to input failure_reason
                # arm_info.failure_reason = ""
                arm_info.failure_time = HAPUtils.get_current_hatohol_time()
                arm_info.num_failure += 1

            self.sender.update_arm_info(arm_info)
            time.sleep(sleep_time)


class HAPZabbixDaemon:
    def __init__(self, host, port, vhost, queue_name, user_name,
                 user_password):
        self.host = host
        self.port = port
        self.vhost = vhost
        self.queue_name = queue_name
        self.user_name = user_name
        self.user_password = user_password

    def start(self):
        poller_queue = multiprocessing.JoinableQueue()
        poller_requested_ids = set()
        main_request_queue = multiprocessing.JoinableQueue()
        main_response_queue = multiprocessing.JoinableQueue()
        main_requested_ids = set()

        receiver = HAPBaseReceiver(self.host,
                                   self.port,
                                   self.vhost,
                                   "r_"+self.queue_name,
                                   self.user_name,
                                   self.user_password,
                                   poller_queue,
                                   main_request_queue,
                                   main_response_queue)

        receive_process =													\
            multiprocessing.Process(target=receiver.connector.run_receive_loop)
        receive_process.daemon = True
        receive_process.start()

        poller = HAPZabbixPoller(self.host,
                                 self.port,
                                 self.vhost,
                                 "s_"+self.queue_name,
                                 self.user_name,
                                 self.user_password,
                                 poller_queue)
        main_plugin = HAPZabbixMainPlugin(self.host,
                                          self.port,
                                          self.vhost,
                                          "s_"+self.queue_name,
                                          self.user_name,
                                          self.user_password,
                                          main_request_queue,
                                          main_response_queue,
                                          poller.sender.ms_info)

        poll_process = multiprocessing.Process(target=poller.poll)
        poll_process.daemon = True
        poll_process.start()

        main_plugin.get_request_loop()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--host",
                        dest="host",
                        type=str,
                        default="localhost",
                        help="RabbitMQ host")
    parser.add_argument("--vhost",
                        dest="vhost",
                        type=str,
                        default=None,
                        help="RabbitMQ vhost")
    parser.add_argument("--port",
                        dest="port",
                        type=int,
                        default=None,
                        help="RabbitMQ port")
    parser.add_argument("--user_name",
                        dest="user_name",
                        type=str,
                        required=True,
                        help="RabbitMQ host user name")
    parser.add_argument("--user_password",
                        dest="user_password",
                        type=str,
                        required=True,
                        help="RabbitMQ host user password")
    parser.add_argument("--queue",
                        dest="queue_name",
                        type=str,
                        required=True,
                        help="RabbitMQ queue")
    args = parser.parse_args()

    #with daemon.DaemonContext():
    hap_zabbix_daemon = HAPZabbixDaemon(args.host, args.port, args.vhost,
                                            args.queue_name, args.user_name,
                                            args.user_password)
    hap_zabbix_daemon.start()
