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
from transporter import Transporter
import transporter

class TestTransporter(unittest.TestCase):

    def test_factory(self):
      obj = transporter.Factory.create(Transporter)
      self.assertEquals(obj.__class__.__name__, "Transporter")

    def test_add_receiver(self):
      def receiver():
          pass

      def receiver2():
          pass

      tx = transporter.Factory.create(Transporter)
      rpc_name = "rpc_name"
      self.assertIsNone(tx._receivers.get(rpc_name))

      tx.add_receiver(rpc_name, receiver)
      receiver_list = tx._receivers.get(rpc_name)
      self.assertEquals(receiver_list, [receiver])

      # add receiver
      tx.add_receiver(rpc_name, receiver2)
      receiver_list = tx._receivers.get(rpc_name)
      self.assertEquals(receiver_list, [receiver, receiver2])
