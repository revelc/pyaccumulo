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
from mock import Mock
from pyaccumulo.iterators import *
from pyaccumulo.proxy.ttypes import IteratorSetting, IteratorScope

class BaseIteratorTest(unittest.TestCase):
    def test_iterator(self):
        i = BaseIterator(name="test123", priority=111, classname="com.test.Class")
        self.assertEquals("test123", i.name)
        self.assertEquals(111, i.priority)
        self.assertEquals("com.test.Class", i.classname)

        a = i.get_iterator_setting()
        self.assertEquals("test123", a.name)
        self.assertEquals(111, a.priority)
        self.assertEquals("com.test.Class", a.iteratorClass)
        self.assertEquals({}, a.properties)

    def test_attach(self):
        i = BaseIterator(name="test123", priority=111, classname="com.test.Class")

        it_s = i.get_iterator_setting()
        scopes = set([IteratorScope.SCAN, IteratorScope.MINC, IteratorScope.MAJC])

        conn =  Mock()
        conn.client = Mock()
        conn.client.attachIterator = Mock()
        conn.login = "Login"

        i.attach(conn, "mytable123", scopes)
        conn.client.attachIterator.assert_called_with("Login", "mytable123", it_s, scopes)

class BaseCombinerTest(unittest.TestCase):
    def test_iterator(self):
        i = BaseCombiner(name="test123", priority=111, classname="com.test.Class", columns=[["a", "b"], ["z", "y"]], encoding_type="STRING")
        self.assertEquals("test123", i.name)
        self.assertEquals(111, i.priority)
        self.assertEquals("com.test.Class", i.classname)
        self.assertEquals([["a", "b"], ["z", "y"]], i.columns)
        self.assertEquals(False, i.combine_all_columns)
        self.assertEquals("STRING", i.encoding_type)

        a = i.get_iterator_setting()
        self.assertEquals("test123", a.name)
        self.assertEquals(111, a.priority)
        self.assertEquals("com.test.Class", a.iteratorClass)
        self.assertEquals(
            {
                "type":"STRING",
                "all": "false",
                "columns": "a:b,z:y"
            }, 
            a.properties)

        i.add_column(colf="c")
        self.assertEquals([["a", "b"], ["z", "y"], ["c"]], i.columns)
        
        i.add_column(colf="c", colq="d")
        self.assertEquals([["a", "b"], ["z", "y"], ["c"], ["c", "d"]], i.columns)


    def test_encode_col(self):
        i = BaseCombiner(name="test123", priority=111, classname="com.test.Class")
        self.assertEquals("a", i._encode_column( ["a"] ) )
        self.assertEquals("a:b", i._encode_column( ["a", "b"] ) )


class SummingCombinerTest(unittest.TestCase):
    def test_iterator(self):
        i = SummingCombiner()
        self.assertEquals("org.apache.accumulo.core.iterators.user.SummingCombiner", i.classname)

class SummingArrayCombinerTest(unittest.TestCase):
    def test_iterator(self):
        i = SummingArrayCombiner()
        self.assertEquals("org.apache.accumulo.core.iterators.user.SummingArrayCombiner", i.classname)

class MaxCombinerTest(unittest.TestCase):
    def test_iterator(self):
        i = MaxCombiner()
        self.assertEquals("org.apache.accumulo.core.iterators.user.MaxCombiner", i.classname)

class MinCombinerTest(unittest.TestCase):
    def test_iterator(self):
        i = MinCombiner()
        self.assertEquals("org.apache.accumulo.core.iterators.user.MinCombiner", i.classname)

class GrepIteratorTest(unittest.TestCase):
    def test_iterator(self):
        i = GrepIterator(term="grep")
        self.assertEquals("grep", i.term)
        self.assertEquals(False, i.negate) 
        self.assertEquals("org.apache.accumulo.core.iterators.user.GrepIterator", i.classname)

        a = i.get_iterator_setting()
        self.assertEquals(
            {
                "term":"grep",
                "negate": "false"
            }, 
            a.properties)

class RowDeletingIteratorTest(unittest.TestCase):
    def test_iterator(self):
        i = RowDeletingIterator()
        self.assertEquals("org.apache.accumulo.core.iterators.user.RowDeletingIterator", i.classname)

class RegExFilterTest(unittest.TestCase):
    def test_iterator(self):
        i = RegExFilter()
        self.assertEquals("org.apache.accumulo.core.iterators.user.RegExFilter", i.classname)

        i = RegExFilter(row_regex="xyz", cf_regex="abc", cq_regex="def", val_regex="jkl", or_fields=True, match_substring=False)
        self.assertEquals("xyz", i.row_regex)
        self.assertEquals("abc", i.cf_regex)
        self.assertEquals("def", i.cq_regex)
        self.assertEquals("jkl", i.val_regex)
        self.assertEquals(True, i.or_fields)
        self.assertEquals(False, i.match_substring)

        self.assertEquals(
            {
                "rowRegex": "xyz",
                "colfRegex": "abc",
                "colqRegex": "def",
                "valueRegex": "jkl",
                "orFields": "true",
                "matchSubstring": "false",
            }, 
            i._get_iterator_properties())

class IntersectingIteratorTest(unittest.TestCase):
    def test_iterator(self):
        i = IntersectingIterator(terms=["quick", "brown", "fox"], not_flags=[False, True, False])
        self.assertEquals("org.apache.accumulo.core.iterators.user.IntersectingIterator", i.classname)
        self.assertEquals(["quick", "brown", "fox"], i.terms)
        self.assertEquals([False, True, False], i.not_flags)

        self.assertEquals("\001", i._convert_flag(True))
        self.assertEquals("\0", i._convert_flag(False))

        self.assertEquals("\001\0\001".encode("base64"), i._encode_not_flags([True, False, True]))
        self.assertEquals("\0\001\0".encode("base64"), i._encode_not_flags([False, True, False]))
        self.assertEquals(None, i._encode_not_flags([]))

        self.assertEquals(
            "quick".encode("base64") + "brown".encode("base64")+ "fox".encode("base64").rstrip("\n"), 
            i._encode_columns(["quick", "brown", "fox"]))

        self.assertEquals( 
            {
                "columnFamilies": "quick".encode("base64") + "brown".encode("base64")+ "fox".encode("base64").rstrip("\n"), 
                "notFlag": "\0\001\0".encode("base64")
            },
            i._get_iterator_properties()
        )

class IndexedDocIteratorTest(unittest.TestCase):
    def test_iterator(self):
        i = IndexedDocIterator(terms=["quick", "brown", "fox"], not_flags=[False, True, False], index_colf="index", doc_colf="docs")
        self.assertEquals("org.apache.accumulo.core.iterators.user.IndexedDocIterator", i.classname)
        self.assertEquals(["quick", "brown", "fox"], i.terms)
        self.assertEquals([False, True, False], i.not_flags)
        self.assertEquals("index", i.index_colf)
        self.assertEquals("docs", i.doc_colf)

        self.assertEquals( 
            {
                "columnFamilies": "quick".encode("base64") + "brown".encode("base64")+ "fox".encode("base64").rstrip("\n"), 
                "notFlag": "\0\001\0".encode("base64"),
                "indexFamily": "index", 
                "docFamily": "docs"
            },
            i._get_iterator_properties()
        )

#----------------------------------------------

def main():
    unittest.main()

if __name__ == '__main__':
    main()
