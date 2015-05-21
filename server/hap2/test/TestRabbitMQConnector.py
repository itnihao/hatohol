#!/usr/bin/env python
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
import unittest
import subprocess
from rabbitmqconnector import RabbitMQConnector

class TestRabbitMQConnector(unittest.TestCase):
    """
    Before executing this test, some setting for RabbitMQ is needed.
    See README in the same directory.
    """
    @classmethod
    def setUpClass(cls):
        cls._broker = "localhost"
        cls._port = None
        cls._vhost = "test"
        cls._queue_name = "test_queue"
        cls._user_name  = "test_user"
        cls._password   = "test_password"

    def test_connect(self):
        conn = RabbitMQConnector()
        conn.connect(self._broker, self._port, self._vhost, self._queue_name,
                     self._user_name, self._password)

    def test_call(self):
        TEST_BODY = "CALL TEST"
        self._delete_test_queue()
        conn = self._create_connected_connector()
        conn.call(TEST_BODY)
        self.assertEqual(self._get_from_test_queue(), TEST_BODY)

    def test_reply(self):
        TEST_BODY = "REPLY TEST"
        self._delete_test_queue()
        conn = self._create_connected_connector()
        conn.reply(TEST_BODY)
        self.assertEqual(self._get_from_test_queue(), TEST_BODY)

    def test_run_receive_loop_without_connect(self):
        conn = RabbitMQConnector()
        self.assertRaises(AssertionError, conn.run_receive_loop)

    def test_run_receive_loop(self):
        class Receiver():
            def __call__(self, channel, msg):
                self.msg = msg
                channel.stop_consuming()

        TEST_BODY = "FOO"
        self._delete_test_queue()
        conn = RabbitMQConnector()
        receiver = Receiver()
        conn.set_receiver(receiver)
        conn.connect(self._broker, self._port, self._vhost, self._queue_name,
                     self._user_name, self._password)
        self._publish(TEST_BODY)
        conn.run_receive_loop()
        self.assertEquals(receiver.msg, TEST_BODY)

    def _create_connected_connector(self):
        conn = RabbitMQConnector()
        conn.connect(self._broker, self._port, self._vhost, self._queue_name,
                     self._user_name, self._password)
        return conn

    def _execute(self, args):
        subproc = subprocess.Popen(args, stdout=subprocess.PIPE)
        output = subproc.communicate()[0]
        self.assertEquals(subproc.returncode, 0)
        return output

    def _build_broker_url(self):
        return "amqp://%s:%s@%s/%s" % (self._user_name, self._password,
                                       self._broker, self._vhost)

    def _publish(self, body):
        args = ["amqp-publish", "-u", self._build_broker_url(),
                "-r", self._queue_name, "-b", body]
        subprocess.Popen(args).communicate()

    def _get_from_test_queue(self):
        args = ["amqp-get", "-u", self._build_broker_url(),
                "-q", self._queue_name]
        return self._execute(args)

    def _delete_test_queue(self):
        args = ["amqp-delete-queue", "-u", self._build_broker_url(),
                "-q", self._queue_name]
        subprocess.Popen(args).communicate()
