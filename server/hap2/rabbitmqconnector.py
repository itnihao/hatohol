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

import logging
import pika
from transporter import Transporter

class RabbitMQConnector(Transporter):

    def __init__(self):
        Transporter.__init__(self)
        self._channel = None

    def connect(self, broker, port, vhost, queue_name, user_name, password):
        """
        @param broker     A broker address.
        @param port       A broker port.
        @param vhost      A virtual host.
        @param queue_name A queue name.
        @param user_name  A user name.
        @param password   A password.
        """

        logging.debug("Called stub method: call().");
        self._queue_name = queue_name
        credentials = pika.credentials.PlainCredentials(user_name, password)
        param = pika.connection.ConnectionParameters(host=broker, port=port,
                                                     virtual_host=vhost,
                                                     credentials=credentials)
        connection = pika.adapters.blocking_connection.BlockingConnection(param)
        self._channel = connection.channel()
        self._channel.queue_declare(queue=queue_name)

    def call(self, msg):
        self._publish(msg)

    def reply(self, msg):
        self._publish(msg)

    def run_receive_loop(self):
        assert self._channel != None

        self._channel.basic_consume(self._consume_handler,
                                    queue=self._queue_name, no_ack=True)
        self._channel.start_consuming()

    def _consume_handler(self, ch, method, properties, body):
        receiver = self.get_receiver()
        if receiver is None:
            logging.warning("Receiver is not registered.")
            return
        receiver(self._channel, body)

    def _publish(self, msg):
        self._channel.basic_publish(exchange="", routing_key=self._queue_name,
                                    body=msg)

