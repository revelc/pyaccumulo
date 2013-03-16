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

class BaseIteratorTest(unittest.TestCase):
    pass

class BaseCombinerTest(unittest.TestCase):
    pass

class SummingCombinerTest(unittest.TestCase):
    pass

class SummingArrayCombinerTest(unittest.TestCase):
    pass

class MaxCombinerTest(unittest.TestCase):
    pass

class MinCombinerTest(unittest.TestCase):
    pass

class GrepIteratorTest(unittest.TestCase):
    pass

class RegExFilterTest(unittest.TestCase):
    pass

class IntersectingIteratorTest(unittest.TestCase):
    pass

class IndexedDocIteratorTest(unittest.TestCase):
    pass



#----------------------------------------------

def main():
    unittest.main()

if __name__ == '__main__':
    main()
