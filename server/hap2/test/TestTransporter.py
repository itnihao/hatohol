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

    def test_factory_with_args(self):
        class DummyTransporter:
            def __init__(self, arg1, arg2=None, arg3=None):
                self.args = [arg1, arg2, arg3]

        obj = transporter.Factory.create(DummyTransporter, 1, arg3=2)
        self.assertEquals(obj.__class__.__name__, "DummyTransporter")
        self.assertEquals(obj.args, [1, None, 2])

    def test_get_receiver(self):
        tx = transporter.Factory.create(Transporter)
        self.assertIsNone(tx.get_receiver())

    def test_set_receiver(self):
        def receiver():
            pass

        def receiver2():
            pass

        tx = transporter.Factory.create(Transporter)
        tx.set_receiver(receiver)
        self.assertEquals(tx.get_receiver(), receiver)

        # update
        tx.set_receiver(receiver2)
        self.assertEquals(tx.get_receiver(), receiver2)
