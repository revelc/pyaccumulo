pyaccumulo
==========

A python client library for Apache Accumulo

Licensed under the Apache 2.0 License

This is still a work in progress.  Pull requests are welcome.

## Requirements

1. A running Accumulo cluster
2. The new Accumulo Thrift Proxy (https://issues.apache.org/jira/browse/ACCUMULO-482) running
3. Thrift python lib installed

## Installation

    pip install thrift
    git clone git@github.com:accumulo/pyaccumulo.git

## Basic Usage

### Creating a connection

    from pyaccumulo import Accumulo, Mutation, Range
    conn = Accumulo(host="my.proxy.hostname", port=50096, user="root", password="secret")

### Basic Table Operations

    table = "mytable"
    if not conn.table_exists(table):
        conn.create_table(table)

### Writing Mutations with a BatchWriter (Batched and optimized for throughput)

    wr = conn.create_batch_writer(table)
    for num in range(0, 1000):
        m = Mutation("row_%d"%num)
        m.put(cf="cf1", cq="cq1", val="%d"%num)
        m.put(cf="cf2", cq="cq2", val="%d"%num)
        wr.add_mutation(m)
    wr.close()

### Simple writes (immediate and syncronous)

    for num in range(0, 1000):
        m = Mutation("row_%d"%num)
        m.put(cf="cf1", cq="cq1", val="%d"%num)
        m.put(cf="cf2", cq="cq2", val="%d"%num)
        conn.write(table, m)

### Scanning a Table
    
    # scan the entire table
    for entry in conn.scan(table):
        print entry.row, entry.cf, entry.cq, entry.cv, entry.ts, entry.val

    # scan() and batch_scan() return a named tuple of (row, cf, cq, cv, ts, val)

    # scan only a portion of the table
    for entry in conn.scan(table, scanrange=Range(srow='row_1', erow='row_2'), cols=[["cf1"]]):
        print entry.row, entry.cf, entry.cq, entry.cv, entry.ts, entry.val

### Using a Batch Scanner

    # scan the entire table with 10 threads
    for entry in conn.batch_scan(table, numthreads=10):
        print entry.row, entry.cf, entry.cq, entry.cv, entry.ts, entry.val
    
## Running the Examples

Run these commands once before running any of the examples.  

    cd pyaccumulo
    vi settings.py # change these settings to match your proxy HOST/PORT and USER/PASSWORD
    export PYTHONPATH="."
    
### Example of simple ingest and scanning

    python examples/simple.py    
    
### Example use of Combiners for Analytics    
    
    python examples/analytics.py    

### Example use Intersecting Iterator for search
    
    python examples/intersecting_iterator.py

### Example use Document Intersecting Iterator for search
    
    python examples/doc_search.py
    
### Example use of Regex Filter for regex based searching

    python examples/regex_search.py

