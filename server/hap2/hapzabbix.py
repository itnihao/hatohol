#! /usr/bin/env python
# coding: UTF-8

import daemon
import json
import multiprocessing
import argparse
import time

import haplib
import zabbixapi

class PreviousHostsInfo:
    def __init__():
        self.hosts = list()
        self.host_groups = list()
        self.host_group_membeship = list()


class HAPZabbixRabbitMQConsumer(haplib.RabbitMQConsumer, haplib.PluginProcedures):
    def __init__(self, host, port, queue_name, user_name, user_password, consumer_queue, publisher_queue):
        RabbitMQConsumer.__init__(self, host, port, "c_" + queue_name, user_name,
                                  user_password)
        self.consumer_queue = consumer_queue
        self.publisher_queue = publisher_queue
        self.publisher = HAPZabbixRabbitMQPublisher(host, port,
                                                    "p_" + queue_name,
                                                    user_name, user_password,
                                                    queue)

    # basic_consume insert the following arguments to callback function.
    # But I don't use other than body.
    def callback_handler(self, ch, method, properties, body):
        valid_json_dict = check_request(body)
        if valid_json_dict is None:
            return

        try:
            eval("self." + valid_json_dict["method"])(valid_json_dict["params"],
                                                      valid_json_dict["id"])
        except KeyError:
            if valid_json_dict["id"] in self.requested_ids:
                consumer_queue.put(valid_json_dict)
            else:
                publisher_queue.put(valid_json_dict)


    def exchangeProfile(self, params, request_id):
        haplib.optimize_server_procedures(SERVER_PROCEDURES, params)
        my_procedures = haplib.get_implement_procedures(HapZabbixProcedures)
        self.exchange_profile(my_procedures, request_id)


    def fetchItems(self, params, request_id):
        self.send_response_to_queue("SUCCESS", request_id)
        self.publisher.put_items(params["hostId"], params["fetchId"])


    def fetchHistory(self, params, request_id):
        self.send_response_to_queue("SUCCESS", request_id)
        self.publisher.put_history(params["itemId"], params["fetchId"])


    def fetchTriggers(self, params, request_id):
        self.send_response_to_queue("SUCCESS", request_id)
        self.publisher.update_triggers(params["lastChangeTime"], params["hostId"],
                                 params["fetchId"])


    def fetchEvents(self, params, request_id):
        self.send_response_to_queue("SUCCESS", request_id)
        self.publisher.update_events(params["lastInfo"], params["count"],
                                 params["direction"], params["fetchId"])


class HAPZabbixRabbitMQPublisher(haplib.RabbitMQPublisher):
    def __init__(self, host, port, queue_name, user_name, user_password, queue):
        RabbitMQPublisher.__init__(self, host, port, queue_name, user_name, user_password)
        self.queue = queue
        ms_dict = self.get_monitoring_server_info()
        self.ms_info = haplib.MonitoringServerInfo(ms_dict)
        self.api = zabbixapi.ZabbixAPI(self.ms_info)
        self.previous_hosts_info = PreviousHostsInfo()
        self.trigger_last_info = None
        self.event_last_info = None
        self.arminfo = haplib.ArmInfo()


    def get_monitoring_server_info(self):
        params = ""
        request_id = get_and_save_request_id(self.requested_ids)
        self.send_request_to_queue("getMonitoringServerInfo", params, request_id)
        return self.get_response_and_check_id(request_id)


    def get_last_info(self, element):
        params = element
        request_id = get_and_save_request_id(self.requested_ids)
        self.send_request_to_queue("getLastInfo", params, request_id)

        return self.get_response_and_check_id(request_id)


    def exchange_profile(self, procedures, response_id=None):
        if response_id is None:
            request_id = get_and_save_request_id(self.requested_ids)
            self.send_request_to_queue("exchangeProfile", procedures, request_id)
            self.get_response_and_check_id(request_id)
        else:
            self.send_response_to_queue(procedures, response_id)


    def put_items(self, host_id = None, fetch_id = None):
        params = {"items": self.api.get_items(host_id)}
        if fetch_id is not None:
            params["fetchId"] = fetch_id

        request_id = haplib.get_and_save_request_id(self.requested_ids)
        self.send_request_to_queue("putItems", params, request_id)

        self.get_response_and_check_id(request_id)


    def put_history(self, item_id, fetch_id):
        params = {"itemId": item_id, "histories": self.api.get_history(item_id),
                  "fetchId": fetch_id}

        request_id = haplib.get_and_save_request_id(self.requested_ids)
        self.send_request_to_queue("putHistory", params, request_id)

        self.get_response_and_check_id(request_id)


    def update_hosts_and_host_group_membership(self, previous_hosts, previous_host_group_membership):
        hosts, hg_membership = self.api.get_hosts()

        hosts.sort()
        if previous_hosts != hosts:
            hosts_params = {"updateType": "ALL", "hosts": hosts}
            request_id = haplib.get_and_save_request_id(self.requested_ids)
            self.send_request_to_queue("updateHosts", params, request_id)
            self.get_response_and_check_id(request_id)
            previous_hosts = hosts

        hg_membership.sort()
        if previous_host_group_membership != hg_membership:
            hg_membership_params = {"updateType": "ALL", "hostGroupMembership": hg_membership}
            request_id = haplib.get_and_save_request_id(self.requested_ids)
            self.send_request_to_queue("updateHostGroupMembership", params, request_id)
            self.get_response_and_check_id(request_id)
            previous_host_group_membership = hg_membership


    def update_host_groups(self, previous_host_groups):
        host_groups = api.get_host_groups()
        host_groups.sort()
        if previous_host_groups != host_groups:
            hosts_params = {"updateType": "ALL", "hostGroups": host_groups}
            request_id = haplib.get_and_save_request_id(self.requested_ids)
            self.send_request_to_queue("updateHostGroups", params, request_id)
            self.get_response_and_check_id(request_id)
            previous_host_groups = host_groups


    def update_triggers(self, requests_since = None, host_id = None, fetchId = None):
        if self.trigger_last_info is None:
            self.trigger_last_info = self.get_last_info("trigger")

        triggers = self.api.get_triggers(self.trigger_last_info, host_id)
        self.trigger_last_info = haplib.find_last_info_from_dict_array(triggers, "lastChangeTime")

        params = {"triggers": triggers, "updateType": "UPDATED",
                  "lastInfo": self.trigger_last_info}

        if fetch_id is not None:
            params["fetchId"] = fetch_id
            params["updateType"] = "ALL"

        request_id = haplib.get_and_save_request_id(self.requested_ids)
        self.send_request_to_queue("updateTriggers", params, request_id)
        self.get_response_and_check_id(request_id)


    def update_events(self, last_info=None, count=None, direction="ASC", fetch_id=None):
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

        count =  len(events)/1000 + 1
        for num in range(0, count):
            start = num * 1000
            send_events = events[start: start + 1000]
            last_info = haplib.find_last_info_from_dict_array(send_events,
                                                              "eventId")
            params = {"events": send_events, "lastInfo": last_info, "updateType": "UPDATE"}

            if fetch_id is not None:
                params["fetchId"] = fetch_id

            if num < count - 1:
                params["mayMoreFlag"] = True

            request_id = haplib.get_and_save_request_id(self.requested_ids)
            self.send_request_to_queue("updateTriggers", params, request_id)
            self.get_response_and_check_id(request_id)

        self.event_last_info = last_info


    def update_arm_info(self, arm_info):
        params = {"lastStatus": arm_info.last_status,
                  "failureReason": arm_info.failure_reason,
                  "lastSuccessTime": arm_info.last_success_time,
                  "lastFailureTime": arm_info.last_failure_time,
                  "numSuccess": arm_info.num_success,
                  "numFailure": arm_info.num_failure}

        request_id = haplib.get_and_save_request_id(self.requested_ids)
        self.send_request_to_queue("updateArmInfo", params, request_id)
        self.get_response_and_check_id(request_id)


    def get_response_and_check_id(request_id):
        # We should set time out in this loop condition.
        while True:
            response_dict = self.queue.get()
            if request_id == response_dict["id"]:
                self.requested_ids.remove(request_id)

                return response_dict["result"]


    def routine_update(self):
        print "Not implement"


class HAPZabbixDaemon:
    def __init__(self, host, port, queue_name, user_name, user_password):
        self.host = host
        self.port = port
        self.queue_name = queue_name
        self.user_name = user_name
        self.user_password = user_password


    def start(self):
        publisher_queue = multioprocessing.Queue()
        consumer_queue = multioprocessing.Queue()
        consumer = HAPZabbixRabbitMQConsumer(self.host, self.port, self.queue_name,
                                             self.user_name, self.user_password, queue)
        subprocess = multiprocessing.Process(target = consumer.start_receiving)
        subprocess.daemon = True
        subprocess.start()

        poll(publisher_queue)


    def poll(publisher_queue):
        publisher = HAPZabbixRabbitMQPublisher(self.host, self.port, "p_"+self.queue_name,
                                               self.user_name, self.user_password, publisher_queue)
        arm_info = haplib.ArmInfo()

        while True:
            sleep_time = publisher.ms_info.interval_sec
            try:
                publisher.routine_update()
                arm_info.last_status = "OK"
                arm_info.failure_reason = ""
                arm_info.last_success_time = haplib.get_hatohol_current_time()
                arm_info.num_success += 1
            except:
                sleep_time = publisher.ms_info.retry_interval_sec
                arm_info.last_status = "NG"
                #ToDo Think about how to input failure_reason
                # arm_info.failure_reason = ""
                arm_info.failure_time = haplib.get_hatohol_current_time()
                arm_info.num_failure += 1

            publisher.update_arm_info(arm_info)
            time.sleep(sleep_time)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--host",
                        dest="host",
                        type=str,
                        default="localhost",
                        help="RabbitMQ host")
    parser.add_argument("--port",
                        dest="rabbitmq_port",
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

    with daemon.DaemonContext():
        hap_zabbix_daemon = HAPZabbixDaemon(args.host, args.port, args.queue_name,
                                            args.user_name, args.user_password)
        hap_zabbix_daemon.start()
