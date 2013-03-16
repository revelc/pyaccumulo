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

from proxy.ttypes import IteratorSetting, IteratorScope
from util import hashcode
import hashlib, re

conn = Accumulo()

table = "search"
if conn.table_exists(table):
    conn.delete_table(table)
conn.create_table(table)

wr = conn.create_batch_writer(table)

license_file = "LICENSE"
linenum = 0

with file(license_file) as infile:
    for line in infile:
        linenum += 1
        line = line.strip()
        uuid = str(linenum)

        m = Mutation(uuid)
        m.put(cf="e", cq="", val=line)
        wr.add_mutation(m)

        m = Mutation("s%02d"% ((hashcode(uuid) & 0x0ffffffff)%4))
        for tok in set(re.split('[\W]+', line.lower())):
            m.put(tok, cq=uuid, val="")
        wr.add_mutation(m)
wr.close()

uuids = []
for e in conn.batch_scan(table, scanranges=[Range(srow="s0", erow="s1")], iterators=[IntersectingIterator(priority=21, terms=["software", "source", "code"])]):
    uuids.append(e.cq)

for doc in conn.batch_scan(table, scanranges=[Range(srow=uuid, erow=uuid) for uuid in uuids]):
    print doc

conn.close()