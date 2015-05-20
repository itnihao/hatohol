#! /usr/lib/env python
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
import pika
import multiprocessing
import random

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


class PluginProcedures:
    def exchangeProfile(self):
        print "Not implement"


    def fetchItems(self):
        print "Not implement"


    def fetchHistory(self):
        print "Not implement"


    def fetchTriggers(self):
        print "Not implement"


    def fetchEvents(self):
        print "Not implement"


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


class RabbitMQConnector:
    def __init__(self, host, port, queue_name, user_name, user_password):
        self.queue_name = queue_name
        credentials = pika.PlaneCredentials(user_name, user_password)
        param = pika.ConnectionParameter(host=host, port=port, credentials=credentials)
        connection = pika.BlockingConnenction(param)
        self.channel = connection.channel()
        self.channel.queue_declare(queue=queue_name)


class RabbitMQPublisher(RabbitMQConnector):
    def __init__(self, host, port, queue_name, user_name, user_password):
        RabbitMQConnector.__init__(self, host, port, queue_name, user_name, user_password)
        self.requested_ids = set()


    def send_request_to_queue(procedure_name, params, request_id):
        request = json.dumps({"jsonrpc": "2.0", "method": procedure_name,
                              "params": params, "id": request_id})
        self.channel.basic_publish(exchange="",
                                   routing_key=self.queue_name,
                                   body=request)


    def send_response_to_queue(result, response_id):
        response = json.dumps({"jsonrpc": "2.0", "result": result,
                               "id": request_id})
        self.channel.basic_publish(exchange="",
                                   routing_key=self.queue_name,
                                   body=response)


class RabbitMQConsumer(RabbitMQConnector):
    def __init__(self, host, port, queue_name, user_name, user_password):
        RabbitMQConnector.__init__(self, host, port, queue_name, user_name, user_password)
        self.plugin_procedures = PluginProcedures()
        self.procedures_instance_name = "self.plugin_procedures"


    def callback_handler(ch, method, properties, body):
        valid_json_dict = self.check_json(body)
        if valid_json_dict is None:
            return

        eval(self.procedures_instance_name + valid_json_dict["method"])(valid_json_dict)


    def start_receiving(self):
        self.channel.basic_consume(self.callback_handler,
                                   queue=self.queue_name, no_ack=True)
        self.channel.start_consuming()


    def check_json(self, json_string):
        json_dict = convert_string_to_dict(json_string)
        if not isinstance(json_dict, dict):
            send_json_to_que(create_error_json(json_dict))
            return

        result = check_implement_method(json_dict["method"])
        if result is not None:
            send_json_to_que(create_error_json(result, json_dict["id"]))
            return

        result = check_argument_is_correct(json_dict["method"])
        if result is not None:
            send_json_to_que(create_error_json(result, json_dict["id"]))
            return

        return json_dict


def get_error_dict():
    error_dict = {-32700: "Parse error", -32600: "invalid Request",
                  -32601: "Method not found", -32602: "invalid params",
                  -32603: "Internal error"}
    for num in range(-32000, -32100):
        error_dict[str(num)] = "Server error"

    return error_dict


def convert_string_to_dict(json_string):
    try:
        json_dict = json.loads(json_string)
    except Exception:
        return -32700
    else:
        return json_dict


def check_method_is_implemented(method_name):
    for method in get_implement_methods():
        if method_name == method:
            return "IMPLEMENT"
        else:
            return -32601


def check_argument_is_correct(json_dict):
    args = inspect.getargspec(json_dict["method"])
    for argument in json_dict["params"]:
        if argument in args:
            result = "OK"

    return -32602
# ToDo Think about algorithm. In case of param is object.


def get_implement_procedures(class_name):
    procedures = ()
    modules = dir(eval(class_name))
    for module in modules:
        if inspect.ismethod(eval(class_name + "." + module)) and eval("PluginProcedures." + module).im_func != eval(class_name + "." + module).im_func:
            procedures = procedures + (module,)

    return procedures


def create_error_json(error_code, req_id = "null"):
    error_dict = get_error_dictdd()
    #ToDo Create place
    return '{"jsonrpc": "2.0", "error": {"code":' + error_code + ', "message":' + error_dict[error_code] + '}, "id":' + req_id + '"}}'


def get_and_save_request_id(requested_ids):
    request_id = random.randint(1,2048)
    requested_ids.add(request_id)

    return request_id


def translate_unix_time_to_hatohol_time(float_unix_time):
    return datetime.strftime(datetime.fromtimestamp(float_unix_time), "%Y%m%d%H%M%S.%f")


def translate_hatohol_time_to_unix_time(hatohol_time):
    return datetime.strptime(hatohol_unix_time, "%Y%m%d%H%M%S.%f")


def optimize_server_procedures(valid_procedures_dict, procedures):
    for procedure in procedures:
        valid_procedures_dict[procedure] = True


def find_last_info_from_dict_array(target_array, last_info_name):
    last_info = None
    for target_dict in target_array:
        if last_info < target_dict[last_info_name]:
                last_info == target_dict[last_info_name]

    return last_info
