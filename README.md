pyaccumulo
==========

A python client library for Apache Accumulo

Licensed under the Apache 2.0 License

This is still a work in progress.

## Requirements

1. A running Accumulo cluster
2. The new Accumulo Thrift Proxy running
3. Thrift python lib installed

## Installation

    pip install thrift
    
## Usage Examples

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
    

