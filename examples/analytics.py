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
import settings

conn = Accumulo(host=settings.HOST, port=settings.PORT, user=settings.USER, password=settings.PASSWORD)

table = "analytics"

if conn.table_exists(table):
    conn.delete_table(table)
conn.create_table(table)

summing = SummingCombiner(priority=10)
summing.add_column("sum")
summing.add_column("count")
summing.attach(conn, table)

sumarray = SummingArrayCombiner(priority=11)
sumarray.add_column("histo")
sumarray.attach(conn, table)

mincom = MinCombiner(priority=12)
mincom.add_column("min")
mincom.attach(conn, table)

maxcom = MaxCombiner(priority=13)
maxcom.add_column("max")
maxcom.attach(conn, table)

wr = conn.create_batch_writer(table)

for num in range(0, 1000):
    m = Mutation("row")
    m.put(cf="sum", cq="cq", val="%d"%num)
    m.put(cf="count", cq="cq", val="%d"%1)
    m.put(cf="min", cq="cq", val="%d"%num)
    m.put(cf="max", cq="cq", val="%d"%num)
    m.put(cf="histo", cq="cq", val=",".join( [str(x) for x in [1,2,3,4,5,6,7,8,9]]))

    wr.add_mutation(m)
wr.close()

for e in conn.scan(table):
    print e
    
conn.close()