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
    
## Usage Examples

Run these commands once before running any of the examples.  

    git clone git@github.com:accumulo/pyaccumulo.git
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

