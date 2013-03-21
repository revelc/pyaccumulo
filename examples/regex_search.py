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

from pyaccumulo import Accumulo, Mutation, Range
from pyaccumulo.iterators import *

from pyaccumulo.proxy.ttypes import IteratorSetting, IteratorScope
from examples.util import hashcode
import hashlib, re

import settings
conn = Accumulo(host=settings.HOST, port=settings.PORT, user=settings.USER, password=settings.PASSWORD)

table = "regexes"
if conn.table_exists(table):
    conn.delete_table(table)
conn.create_table(table)

wr = conn.create_batch_writer(table)

license_file = "LICENSE"
linenum = 0

with file(license_file) as infile:
    for line in infile:
        linenum += 1
        
        m = Mutation(str(linenum))
        m.put(cf="e", cq="", val=line.strip())
        wr.add_mutation(m)
wr.close()

regex1 = RegExFilter(priority=21, val_regex=".*stated.*", match_substring=True, name="RegExFilter1")
regex2 = RegExFilter(priority=22, val_regex='.*patent', match_substring=True, name="RegExFilter2")
regex3 = RegExFilter(priority=23, val_regex='have made', match_substring=True, name="RegExFilter3")

for e in conn.batch_scan(table, cols=[["e"]], iterators=[regex1, regex2, regex3]):
    print e

conn.close()
