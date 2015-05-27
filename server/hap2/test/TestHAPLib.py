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
import haplib

class SenderForTest(haplib.HAPBaseSender):

    def __init__(self, test_queue, change_get_response_and_check_id_flag=False):
        self.sender_queue = test_queue
        self.connector = ConnectorForTest(test_queue)
        self.requested_ids = set()

        if change_get_response_and_check_id_flag:
            def get_response_and_check_id(self):
                return


class ConnectorForTest:
    def __init__(self, test_queue):
        self.test_queue = test_queue

    def call(request):
        self.test_queue.get()
        self.test_queue.task_done()
        self.test_queue.put({"id": 1, "result": None})
        self.test_queue.task_done()

    def reply(result):
        return

    def set_receiver(func):
        return
