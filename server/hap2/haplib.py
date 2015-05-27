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

import json
from datetime import datetime
import time
import pika
import random
import Queue
import logging

from transporter import Factory
from rabbitmqconnector import RabbitMQConnector

SERVER_PROCEDURES = {"exchangeProfile": True,
                     "getMonitoringServerInfo": True,
                     "getLastInfo": True,
                     "putItems": True,
                     "putHistory": True,
                     "updateHosts": True,
                     "updateHostGroups": True,
                     "updateHostGroupMembership": True,
                     "updateTriggers": True,
                     "updateEvents": True,
                     "updateHostParent": True,
                     "updateArmInfo": True}
ERROR_DICT = {-32700: "Parse error", -32600: "invalid Request",
              -32601: "Method not found", -32602: "invalid params"}

class MonitoringServerInfo:
    def __init__(self, ms_info_dict):
        self.server_id = ms_info_dict["serverId"]
        self.url = ms_info_dict["url"]
        self.server_type = ms_info_dict["type"]
        self.nick_name = ms_info_dict["nickName"]
        self.user_name = ms_info_dict["userName"]
        self.password = ms_info_dict["password"]
        self.polling_interval_sec = ms_info_dict["pollingIntervalSec"]
        self.retry_interval_sec = ms_info_dict["retryIntervalSec"]
        self.extended_info = ms_info_dict["extendedInfo"]


class ArmInfo:
    def __init__(self):
        self.last_status = str()
        self.failure_reason = str()
        self.last_success_time = str()
        self.last_failure_time = str()
        self.num_success = int()
        self.num_failure = int()


class HAPBaseSender:
    def __init__(self, host, port, vhost, queue_name, user_name,
                 user_password, sender_queue, ms_info=None):
        # Currentory, RabbitMQConnector only.
        #I want to add way of select connection to use argument.
        self.connector = Factory.create(RabbitMQConnector)
        self.connector.connect(broker=host, port=port, vhost=vhost,
                               queue_name=queue_name, user_name=user_name,
                               password=user_password)
        self.sender_queue = sender_queue
        self.requested_ids = set()
        if ms_info is None:
            ms_dict = self.get_monitoring_server_info()
            self.ms_info = MonitoringServerInfo(ms_dict)
        else:
            self.ms_info = ms_info

    def send_request_to_queue(self, procedure_name, params, request_id):
        request = json.dumps({"jsonrpc": "2.0", "method": procedure_name,
                              "params": params, "id": request_id})
        self.sender_queue.put(self.requested_ids)
        self.connector.call(request)

    def send_response_to_queue(self, result, response_id):
        response = json.dumps({"jsonrpc": "2.0", "result": result,
                               "id": response_id})
        self.connector.reply(result)

    def send_error_to_queue(self, error_code, response_id):
        response = json.dumps({"jsonrpc": "2.0",
                               "error": {"code": error_code,
                                         "message": ERROR_DICT[error_code]},
                               "id": response_id})
        self.connector.reply(response)

    def get_monitoring_server_info(self):
        params = ""
        request_id = HAPUtils.get_and_save_request_id(self.requested_ids)
        self.send_request_to_queue("getMonitoringServerInfo", params, request_id)
        return self.get_response_and_check_id(request_id)

    def get_last_info(self, element):
        params = element
        request_id = HAPUtils.get_and_save_request_id(self.requested_ids)
        self.send_request_to_queue("getLastInfo", params, request_id)

        return self.get_response_and_check_id(request_id)

    def exchange_profile(self, procedures, response_id=None):
        if response_id is None:
            request_id = HAPUtils.get_and_save_request_id(self.requested_ids)
            self.send_request_to_queue("exchangeProfile", procedures, request_id)
            self.get_response_and_check_id(request_id)
        else:
            self.send_response_to_queue(procedures, response_id)

    def update_arm_info(self, arm_info):
        params = {"lastStatus": arm_info.last_status,
                  "failureReason": arm_info.failure_reason,
                  "lastSuccessTime": arm_info.last_success_time,
                  "lastFailureTime": arm_info.last_failure_time,
                  "numSuccess": arm_info.num_success,
                  "numFailure": arm_info.num_failure}

        request_id = HAPUtils.get_and_save_request_id(self.requested_ids)
        self.send_request_to_queue("updateArmInfo", params, request_id)
        self.get_response_and_check_id(request_id)

    def get_response_and_check_id(self, request_id):
        try:
            self.sender_queue.join()
            response_dict = self.sender_queue.get(True, 30)
            self.sender_queue.task_done()

            if request_id == response_dict["id"]:
                self.requested_ids.remove(request_id)

                return response_dict["result"]
        except ValueError as exception:
            if exception == "task_done() called too many times" and          \
                                            request_id == response_dict["id"]:
                self.requested_ids.remove(request_id)

                return response_dict["result"]
            else:
                return
        except Queue.Empty:
            self.requested_ids.remove(request_id)
            logging.error("Request failed")
            return



class HAPBaseReceiver:
    def __init__(self, host, port, vhost, queue_name, user_name,
                 user_password, poller_queue, main_request_queue,
                 main_response_queue):
        self.main_request_queue = main_request_queue
        self.main_response_queue = main_response_queue
        self.poller_queue = poller_queue
        self.main_requested_ids = set()
        self.poller_requested_ids = set()

        self.connector = Factory.create(RabbitMQConnector)
        self.connector.connect(broker=host, port=port, vhost=vhost,
                               queue_name=queue_name, user_name=user_name,
                               password=user_password)
        self.connector.set_receiver(self.message_manager)

    def message_manager(self, ch, body):
        valid_request = HAPUtils.check_message(body)
        if isinstance(valid_request, tuple):
            self.main_request_queue.put(valid_request)
            return

        try:
            self.poller_requested_ids = self.poller_queue.get(False)
            self.poller_queue.task_done()
        except Queue.Empty:
            #ToDo print to logging
            pass

        try:
            self.main_requested_ids = self.main_response_queue.get(False)
            self.main_requested_ids.task_done()
        except Queue.Empty:
            #ToDo print to logging
            pass

        try:
            if valid_request["id"] in self.poller_requested_ids:
                self.poller_queue.put(valid_request)
            elif valid_request["id"] in self.main_requested_ids:
                self.main_response_queue.put(valid_request)
            else:
                self.main_request_queue.put(valid_request)
        except KeyError:
            #The following sentence is used in case of receive notification.
            self.main_request_queue.put(valid_request)


class HAPBaseMainPlugin:
    def __init__(self, host, port, vhost, queue_name, user_name,
                 user_password, main_request_queue, ms_info=None):
        self.main_request_queue = main_request_queue
        self.sender =  HAPBaseSender(host, port, vhost, queue_name, user_name,
                                     user_password,main_request_queue, ms_info)
        self.procedures = {"exchangeProfile": self.hap_exchange_profile,
                           "fetchItems": self.hap_fetch_items,
                           "fetchHistory": self.hap_fetch_history,
                           "fetchTriggers": self.hap_fetch_triggers,
                           "fetchEvents": self.hap_fetch_events}

    def hap_exchange_profile(self, params, request_id):
        pass

    def hap_fetch_items(self, params, request_id):
        pass

    def hap_fetch_history(self, params, request_id):
        pass

    def hap_fetch_triggers(self, params, request_id):
        pass

    def hap_fetch_events(self, params, request_id):
        pass

    def hap_return_error(self, error_code, response_id):
        self.sender.error_to_queue(error_code, response_id)

    def get_request_loop(self):
        while True:
            request = self.main_request_queue.get()
            try:
                self.procedures[request["method"]](request["params"],
                                                   request["id"])
            except KeyError:
                #The following sentense is used in case of receive notification
                # from Hatohol server. 
                self.procedures[request["method"]](request["params"])
            except ValueError as exception:
                if exception == "tuple indices must be integers, not str":
                    self.hap_return_error(request[0], request[1])


class HAPUtils:

    PROCEDURES_ARGS = {"exchangeProfile": {"procedures":list(), "name": str()},
                       "fetchItems": {"hostIds":list(), "fetchIds": str()},
                       "fetchHistory": {"hostId":str(),"itemId": str(),
                                        "beginTime": str(), "entTime": str(),
                                        "fetchIds": str()},
                       "fetchTriggers": {"hostIds":list(), "fetchIds": str()},
                       "fetchEvents": {"lastInfo":str(),"count":int(),
                                       "direction": str(),"fetchIds": str()}}

    @staticmethod
    def check_message(message, implement_procedures):
        error_code, message_dict = HAPUtils.convert_string_to_dict(message)
        if isinstance(error_code, int):
            return (error_code, None)

        try:
            message_dict["result"]
            message_dict["id"]
            return message_dict
        except KeyError:
            pass

        error_code = HAPUtils.check_procedure_is_implemented(               \
                                  message_dict["method"], implement_procedures)
        if isinstance(error_code, int):
            try:
                return (error_code, message_dict["id"]))
            except KeyError:
                return (error_code, None)

        error_code = HAPUtils.check_argument_is_correct(message_dict["method"])
        if isinstance(error_code, int):
            try:
                return (error_code, message_dict["id"]))
            except KeyError:
                return (error_code, None)

        return message_dict

    @staticmethod
    def convert_string_to_dict(json_string):
        try:
            json_dict = json.loads(json_string)
        except ValueError:
            return (-32700, None)
        else:
            return (None, json_dict)

    @staticmethod
    def check_method_is_implemented(method_name):
        for method in get_implement_methods():
            if method_name == method:
                return "IMPLEMENT"
            else:
                return -32601

    @staticmethod
    def check_argument_is_correct(json_dict):
        args_dict = HAPUtils.PROCEDURES_ARGS(json_dict["method"])
        for arg_name, arg_value in json_dict["params"].itervalues():
            try:
                if type(arg_dict[arg_name]) != type(arg_value):
                    return -32602
            except KeyError:
                return -32602

    @staticmethod
    def get_implement_procedures(class_name):
        procedures = ()
        modules = dir(eval(class_name))
        for module in modules:
            if inspect.ismethod(eval(class_name + "." + module)) and eval("PluginProcedures." + module).im_func != eval(class_name + "." + module).im_func:
                procedures = procedures + (module,)

        return procedures

    @staticmethod
    def get_and_save_request_id(requested_ids):
        request_id = random.randint(1, 2048)
        requested_ids.add(request_id)

        return request_id

    @staticmethod
    def translate_unix_time_to_hatohol_time(float_unix_time):
        return datetime.strftime(datetime.fromtimestamp(float_unix_time), "%Y%m%d%H%M%S.%f")

    @staticmethod
    def translate_hatohol_time_to_unix_time(hatohol_time):
        return datetime.strptime(hatohol_unix_time, "%Y%m%d%H%M%S.%f")

    @staticmethod
    def optimize_server_procedures(valid_procedures_dict, procedures):
        for procedure in procedures:
            valid_procedures_dict[procedure] = True

    @staticmethod
    def find_last_info_from_dict_array(target_array, last_info_name):
        last_info = None
        for target_dict in target_array:
            if last_info < target_dict[last_info_name]:
                    last_info = target_dict[last_info_name]

        return last_info

    @staticmethod
    def get_current_hatohol_time():
        unix_time = float(time.mktime(datetime.now().utctimetuple()))
        return HAPUtils.translate_unix_time_to_hatohol_time(unix_time)
