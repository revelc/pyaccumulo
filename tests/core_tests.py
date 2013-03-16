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

class AccumuloTest(unittest.TestCase):
    def test_get_scan_columns(self):
        pass

    def test_following_array(self):
        pass

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
        pass

class MutationTest(unittest.TestCase):
    def test_put(self):
        pass
        
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
