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
import os

NUM_SHARDS=4

def usage(msg=None):
    print "Usage: %s <table> <dir1> [<dir2> <dir3> ...]"%sys.argv[0]
    sys.exit(1)

def get_uuid(filePath):
    return hashlib.md5(filePath).hexdigest()
    
def get_shard(uuid):
    return "s%02d"% ((hashcode(uuid) & 0x0ffffffff)%NUM_SHARDS)

def get_tokens(f):
    return set([item for sublist in [re.split('[^\w]+', line.lower()) for line in f] for item in sublist if len(item) > 3])

def write_mutations(writer, shard, uuid, value, tokens):
    m = Mutation(shard)
    m.put(cf="e\0file", cq=uuid, val=value)
    for tok in tokens:
        m.put(cf="i", cq="%s\0file\0%s\0info"%(tok, uuid), val="")
        if len(m.updates) > 1000:
            writer.add_mutation(m)
            m = Mutation(shard)

    if len(m.updates) > 0:
        writer.add_mutation(m)

try:
    table = sys.argv[1]
    input_dirs = sys.argv[2:]
except:
    usage()

conn = Accumulo(host=settings.HOST, port=settings.PORT, user=settings.USER, password=settings.PASSWORD)

if not conn.table_exists(table):
    print "Creating table: %s"%table
    conn.create_table(table)

wr = conn.create_batch_writer(table)

for indir in input_dirs:
    for root, subFolders, files in os.walk(indir):
        for filename in files:
            filePath = os.path.join(root, filename)
            print "indexing file %s"%filePath
            uuid = get_uuid(filePath)
            with open( filePath, 'r' ) as f:
                write_mutations(wr, get_shard(uuid), uuid, filePath, get_tokens(f))
wr.close()
