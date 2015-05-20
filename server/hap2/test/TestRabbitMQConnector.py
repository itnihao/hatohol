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

    def test_run_receive_loop_without_connect(self):
        conn = RabbitMQConnector()
        try:
            conn.run_receive_loop()
        except AssertionError:
            pass

    def test_run_receive_loop(self):
        class Receiver():
            def __call__(self, channel, msg):
                self.msg = msg
                channel.stop_consuming()

        TEST_BODY = "FOO"
        conn = RabbitMQConnector()
        receiver = Receiver()
        conn.set_receiver(receiver)
        conn.connect(self._broker, self._port, self._vhost, self._queue_name,
                     self._user_name, self._password)
        # TODO: ensure that the queue is empty
        self._publish(TEST_BODY)
        conn.run_receive_loop()
        self.assertEquals(receiver.msg, TEST_BODY)

    def _build_broker_url(self):
        return "amqp://%s:%s@%s/%s" % (self._user_name, self._password,
                                       self._broker, self._vhost)

    def _publish(self, body):
        args = ["amqp-publish", "-u", self._build_broker_url(),
                "-r", self._queue_name, "-b", body]
        subprocess.Popen(args).communicate()
