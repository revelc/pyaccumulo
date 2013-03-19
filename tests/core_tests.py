#!/usr/bin/env python
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest

import pyaccumulo
from pyaccumulo import *

class AccumuloTest(unittest.TestCase):
    def test_get_scan_columns(self):
        self.assertEquals(None, pyaccumulo._get_scan_columns([]))
        self.assertEquals([ ScanColumn(colFamily="cf", colQualifier="cq") ], pyaccumulo._get_scan_columns([ ["cf", "cq"]]))
        self.assertEquals([ ScanColumn(colFamily="cf", colQualifier="cq"), ScanColumn(colFamily="a") ], pyaccumulo._get_scan_columns([ ["cf", "cq"], ["a"]]))

    def test_following_array(self):
        self.assertEquals("test\0", pyaccumulo.following_array("test"))
        self.assertEquals(None, pyaccumulo.following_array(None))

    def test_list_tables(self):
        pass

    def test_table_exists(self):
        pass

    def test_list_tables(self):
        pass

    def test_create_table(self):
        pass

    def test_delete_table(self):
        pass

    def test_rename_table(self):
        pass

    def test_get_range(self):
        pass

    def test_get_ranges(self):
        pass

    def test_scan(self):
        pass

    def test_batch_scan(self):
        pass

    def test_perform_scan(self):
        pass

    def test_create_batch_writer(self):
        pass

class RangeTest(unittest.TestCase):
    def test_to_range(self):

        r = Range(srow=None, erow=None)
        rng = r.to_range()
        self.assertEquals(None, rng.start)
        self.assertEquals(None, rng.stop)

        r = Range(srow="r01", erow="r02")
        rng = r.to_range()
        self.assertEquals(Key(row="r01"), rng.start)
        self.assertEquals(Key(row="r02\0"), rng.stop)

        r = Range(erow="r02", einclude=False)
        rng = r.to_range()
        self.assertEquals(Key(row="r02"), rng.stop)

        r = Range(srow="r01", sinclude=False)
        rng = r.to_range()
        self.assertEquals(Key(row="r01\0"), rng.start)

        r = Range(srow="r01", scf="cf1", scq="cq1", erow="r02", ecf="cf2", ecq="cq2", sts=100, ets=101, scv="xy", ecv="zx")
        rng = r.to_range()
        self.assertEquals(Key(row="r01", colFamily="cf1", colQualifier="cq1", timestamp=100, colVisibility="xy"), rng.start)
        self.assertEquals(Key(row="r02\0", colFamily="cf2", colQualifier="cq2", timestamp=101, colVisibility="zx"), rng.stop)

class MutationTest(unittest.TestCase):
    def test_mutation(self):
        m = Mutation("row1")
        self.assertEquals("row1", m.row)
        self.assertEquals([], m.updates)

    def test_put(self):
        m = Mutation("row1")
        self.assertEquals([], m.updates)
        m.put(cf="cf1", cq="cq1", cv="xy", ts=101, val="myval")
        self.assertEquals([ColumnUpdate(colFamily="cf1", colQualifier="cq1", colVisibility="xy", timestamp=101, value="myval", deleteCell=None)], m.updates)

        
class BatchWriterTest(unittest.TestCase):
    def test_init(self):
        pass

    def test_add_mutations(self):
        pass

    def test_add_mutation(self):
        pass

    def test_close(self):
        pass

    def test_flush(self):
        pass

#----------------------------------------------

def main():
    unittest.main()

if __name__ == '__main__':
    main()
