# -*- coding: utf-8 -*-

# Copyright 2009-2012 10gen, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Test transactions in mongodb."""

import sys
import threading
import unittest

sys.path[0:0] = [""]

from nose.plugins.skip import SkipTest
from test.test_client import get_client


class TestTransactions(unittest.TestCase):

    def setUp(self):
        self.client = get_client()
        self.db = self.client.pymongo_test

    def tearDown(self):
        self.db.drop_collection("test_large_limit")
        self.db = None
        self.client = None

    def test_cursor_dictionary_too_new(self):
        if self.client.is_mongos:
            raise SkipTest("Can't use transactions through mongos.")
        coll = 'cursor-dictionary-too-new'
        self.db[coll].drop()

        self.assertNotIn(coll, self.db.collection_names())

        def other_thread(self, lk):
            lk.acquire()
            try:
                with self.client.start_request():
                    self.db[coll].insert({'a':1})
                    self.assertIn(coll, self.db.collection_names())
            finally:
                lk.release()

        lk = threading.Lock()
        t = threading.Thread(target=other_thread, args=(self, lk))
        lk.acquire()
        t.start()
        try:
            with self.client.start_request():
                try:
                    self.db.command('beginTransaction')
                    lk.release()
                    lk.acquire()
                    self.assertNotIn(coll, self.db.collection_names())
                    self.assertEqual(0, self.db[coll].count())
                    # Cursor.count() checks for a "ns missing" error and
                    # suppresses it, so also check this way:
                    self.assertTrue(self.db.command('count', coll)['missing'])
                    self.db.command('rollbackTransaction')
                finally:
                    lk.release()
        finally:
            t.join()

if __name__ == "__main__":
    unittest.main()
