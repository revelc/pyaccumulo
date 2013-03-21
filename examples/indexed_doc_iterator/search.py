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
import sys

conn = Accumulo(host=settings.HOST, port=settings.PORT, user=settings.USER, password=settings.PASSWORD)

table = sys.argv[1]
if not conn.table_exists(table):
    print "Table '%s' does not exist."%table
    sys.exit(1)

search_terms = [term.lower() for term in sys.argv[2:] if len(term) > 3]

if len(search_terms) < 2:
    print "More than one term of length > 3 is required for this example"
    sys.exit(1)

for e in conn.batch_scan(table, iterators=[IndexedDocIterator(priority=21, terms=search_terms)]):
    print e.val
conn.close()
