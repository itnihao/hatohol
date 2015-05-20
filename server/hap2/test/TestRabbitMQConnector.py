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
from rabbitmqconnector import RabbitMQConnector

class TestRabbitMQConnector(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._broker = "localhost"
        cls._port = None
        cls._vhost = "/test"
        cls._queue_name = "test_queue"
        cls._user_name  = "test_user"
        cls._password   = "test_password"

    def test_connect(self):
        conn = RabbitMQConnector()
        conn.connect(self._broker, self._port, self._vhost, self._queue_name,
                     self._user_name, self._password)


