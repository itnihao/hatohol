#! /usr/bin/env python
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

class Transporter:
    """
    An abstract class for transportation of RPC messages for HAPI-2.0.
    """

    def __init__(self):
        self._receivers = {}

    def call(self, procedure, params, rpcid):
        """
        Call a RPC.
        @param procedure  A name of the procedure.
        @param params     An object used as 'params'.
        @param rpcid      An ID of the call.
        """
        logging.debug("Called stub method: call().");

    def reply(self, result, rpcid):
        """
        Replay a message to a caller.
        @param result   An object used as 'result'.
        @param rpc      An ID of the call.
        """
        logging.debug("Called stub method: reply().");

    def add_receiver(self, rpc_name, receiver):
        """
        Register a receiver method.
        @rpc_name An RPC name associated with the receiver.
        @receiver A receiver method.
        """
        receiver_list = self._receivers.get(rpc_name)
        if receiver_list is None:
            receiver_list = []
            self._receivers[rpc_name] = receiver_list
        receiver_list.append(receiver)

    def run_receive_loop(self):
        """
        Start receiving. When a message arrives, handlers registered by
        add_receiver() are called. This method doesn't return.
        """
        pass

class Factory:
    @classmethod
    def create(cls, transporter_class):
        """
        Create a transporter object.
        @param name A name of the transporter.
        @return A created transporter object.
        """
        return transporter_class()
