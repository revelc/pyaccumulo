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
import settings

table = "pythontest"

conn = Accumulo(host=settings.HOST, port=settings.PORT, user=settings.USER, password=settings.PASSWORD)

if conn.table_exists(table):
    conn.delete_table(table)

conn.create_table(table)
wr = conn.create_batch_writer(table)

print "Ingesting some data ..."
for num in range(1, 100):
    label = '%03d'%num
    mut = Mutation('r_%s'%label)
    mut.put(cf='cf_%s'%label, cq='cq1', val='value_%s'%label)
    mut.put(cf='cf_%s'%label, cq='cq2', val='value_%s'%label)
    wr.add_mutation(mut)
wr.close()


print "Rows 001 through 003 ..."
for entry in conn.scan(table, scanrange=Range(srow='r_001', erow='r_003'), cols=[]):
    print entry

print "Rows 001 and 011 ..."
for entry in conn.batch_scan(table, scanranges=[Range(srow='r_001', erow='r_001'), Range(srow='r_011', erow='r_011')]):
    print entry

conn.close()
