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

import pyaccumulo
from pyaccumulo import *
from mock import Mock
from pyaccumulo.proxy.ttypes import IteratorScope, PartialKey, SystemPermission, TablePermission


class AccumuloTest(unittest.TestCase):
    def _get_mock_connection(self):
        conn = Accumulo(_connect=False)
        conn.client = Mock()
        conn.login = "Login"
        return conn

    def test_get_scan_columns(self):
        self.assertEquals(None, pyaccumulo._get_scan_columns([]))
        self.assertEquals([ ScanColumn(colFamily="cf", colQualifier="cq") ], pyaccumulo._get_scan_columns([ ["cf", "cq"]]))
        self.assertEquals([ ScanColumn(colFamily="cf", colQualifier="cq"), ScanColumn(colFamily="a") ], pyaccumulo._get_scan_columns([ ["cf", "cq"], ["a"]]))

    def test_following_array(self):
        self.assertEquals("test\0", pyaccumulo.following_array("test"))
        self.assertEquals(None, pyaccumulo.following_array(None))

    def test_list_tables(self):
        conn = self._get_mock_connection()
        conn.client.listTables = Mock()
        conn.client.listTables.return_value = set(["t1", "t2", "t3"])

        res = conn.list_tables()
        conn.client.listTables.assert_called_with("Login")
        self.assertEquals(set(["t1", "t2", "t3"]), set(res))


    def test_table_exists(self):
        conn = self._get_mock_connection()
        conn.client.tableExists = Mock()
        conn.client.tableExists.return_value = True

        res = conn.table_exists("mytable")
        conn.client.tableExists.assert_called_with("Login", "mytable")
        self.assertEquals(True, res)


    def test_create_table(self):
        conn = self._get_mock_connection()
        conn.client.createTable = Mock()
        conn.client.createTable.return_value = True

        conn.create_table("mytable")
        conn.client.createTable.assert_called_with("Login", "mytable", True, TimeType.MILLIS)        

    def test_delete_table(self):
        conn = self._get_mock_connection()
        conn.client.deleteTable = Mock()
        conn.delete_table("mytable")
        conn.client.deleteTable.assert_called_with("Login", "mytable")

    def test_rename_table(self):
        conn = self._get_mock_connection()
        conn.client.renameTable = Mock()
        conn.rename_table("mytable", "newtable")
        conn.client.renameTable.assert_called_with("Login", "mytable", "newtable")

    def test_get_range(self):
        conn = Accumulo(_connect=False)
        r = Mock()
        r.to_range = Mock()
        r.to_range.return_value = "xyz"

        res = conn._get_range(r)
        r.to_range.assert_called_with()
        self.assertEquals("xyz", res)

        res = conn._get_range(None)
        self.assertEquals(None, res)

    def test_get_ranges(self):
        conn = Accumulo(_connect=False)
        r = Mock()
        r.to_range = Mock()
        r.to_range.return_value = "xyz"

        res = conn._get_ranges([r])
        r.to_range.assert_called_with()
        self.assertEquals(["xyz"], res)

        res = conn._get_ranges(None)
        self.assertEquals(None, res)

    def test_write(self):
        conn = Accumulo(_connect=False)

        writer = Mock()
        writer.add_mutations = Mock()
        writer.close = Mock()

        conn.create_batch_writer = Mock(return_value=writer)

        mut = Mutation("r01")
        conn.write("mytable", mut)

        conn.create_batch_writer.assert_called_with("mytable")
        writer.add_mutations.assert_called_with([mut])
        writer.close.assert_called_with()


    def test_scan(self):
        pass

    def test_batch_scan(self):
        pass

    def test_perform_scan(self):
        pass

    def test_get_iterator_settings(self):
        conn = Accumulo(_connect=False)

        res = conn._get_iterator_settings(None)
        self.assertEquals(None, res)

    def test_process_iterator(self):
        conn = Accumulo(_connect=False)

        res = conn._process_iterator(IteratorSetting(name="i1"))
        self.assertEquals(IteratorSetting(name="i1"), res)

        res = conn._process_iterator(BaseIterator(name="n1", priority=21, classname="c1"))
        self.assertEquals(IteratorSetting(priority=21, iteratorClass='c1', name='n1', properties={}), res)

        with self.assertRaises(Exception):
            conn._process_iterator("not an iterator")

    def test_close(self):
        conn = Accumulo(_connect=False)
        conn.transport = Mock()
        conn.transport.close = Mock()
        conn.close()
        conn.transport.close.assert_called_with()

    def test_attach_iterator(self):
        conn = self._get_mock_connection()
        conn.client.attachIterator = Mock()
        iter_setting = IteratorSetting(name="i1")
        scopes = {IteratorScope.MAJC, IteratorScope.MINC, IteratorScope.SCAN}
        conn.attach_iterator("mytable", iter_setting, scopes)
        conn.client.attachIterator.assert_called_with("Login", "mytable", iter_setting, scopes)

    def test_remove_iterator(self):
        conn = self._get_mock_connection()
        conn.client.removeIterator = Mock()
        scopes = {IteratorScope.MAJC, IteratorScope.MINC, IteratorScope.SCAN}
        conn.remove_iterator("mytable", "i1", scopes)
        conn.client.removeIterator.assert_called_with("Login", "mytable", "i1", scopes)

    def test_following_key(self):
        conn = self._get_mock_connection()
        key = Key(row="aabbcc", colFamily="cf", colQualifier="cq")
        conn.client.getFollowing = Mock(return_value=following_key(key))
        fkey = conn.following_key(key, PartialKey.ROW_COLFAM_COLQUAL_COLVIS)
        conn.client.getFollowing.assert_called_with(key, PartialKey.ROW_COLFAM_COLQUAL_COLVIS)
        self.assertEqual(fkey, Key(row="aabbcc", colFamily="cf", colQualifier="cq\0"))

    def test_get_max_row(self):
        conn = self._get_mock_connection()
        conn.client.getMaxRow = Mock(return_value="aabe")
        row = conn.get_max_row("mytable", auths=["auth1", "auth2"], srow="aabb", sinclude=True, erow="bbaa", einclude=False)
        conn.client.getMaxRow.assert_called_with("Login", "mytable", ["auth1", "auth2"], "aabb", True, "bbaa", False)
        self.assertEqual(row, "aabe")

    def test_add_mutations_and_flush(self):
        conn = self._get_mock_connection()
        conn.client.updateAndFlush = Mock()
        muts = [Mutation("aabb"), Mutation("bbaa")]
        conn.add_mutations_and_flush("mytable", muts)
        conn.client.updateAndFlush.assert_called_with("Login", "mytable", {'aabb': [], 'bbaa': []})
        conn.add_mutations_and_flush("mytable", Mutation("ccdd"))
        conn.client.updateAndFlush.assert_called_with("Login", "mytable", {'ccdd': []})

    def test_create_user(self):
        conn = self._get_mock_connection()
        conn.createLocalUser = Mock()
        conn.create_user("user", "password")
        conn.client.createLocalUser.assert_called_with("Login", "user", "password")

    def test_drop_user(self):
        conn = self._get_mock_connection()
        conn.client.dropLocalUser = Mock()
        conn.drop_user("user")
        conn.client.dropLocalUser.assert_called_with("Login", "user")

    def test_list_users(self):
        conn = self._get_mock_connection()
        conn.client.listLocalUsers = Mock(return_value=["user1", "user2"])
        users = conn.list_users()
        conn.client.listLocalUsers.assert_called_with("Login")
        self.assertEqual(users, ["user1", "user2"])

    def test_set_user_authorizations(self):
        conn = self._get_mock_connection()
        conn.client.changeUserAuthorizations = Mock()
        conn.set_user_authorizations("user", ["auth1", "auth2"])
        conn.client.changeUserAuthorizations.assert_called_with("Login", "user", ["auth1", "auth2"])

    def test_get_user_authorizations(self):
        conn = self._get_mock_connection()
        conn.client.getUserAuthorizations = Mock(return_value=["auth1", "auth2"])
        auths = conn.get_user_authorizations("user1")
        conn.client.getUserAuthorizations.assert_called_with("Login", "user1")
        self.assertEqual(auths, ["auth1", "auth2"])

    def test_grant_system_permission(self):
        conn = self._get_mock_connection()
        conn.client.grantSystemPermission = Mock()
        conn.grant_system_permission("user1", SystemPermission.CREATE_TABLE)
        conn.client.grantSystemPermission.assert_called_with("Login", "user1", SystemPermission.CREATE_TABLE)

    def test_revoke_system_permission(self):
        conn = self._get_mock_connection()
        conn.client.revokeSystemPermission = Mock()
        conn.revoke_system_permission("user1", SystemPermission.DROP_USER)
        conn.client.revokeSystemPermission.assert_called_with("Login", "user1", SystemPermission.DROP_USER)

    def test_has_system_permission(self):
        conn = self._get_mock_connection()
        conn.client.hasSystemPermission = Mock(return_value=True)
        has_perm = conn.has_system_permission("user1", SystemPermission.SYSTEM)
        conn.client.hasSystemPermission.assert_called_with("Login", "user1", SystemPermission.SYSTEM)
        self.assertTrue(has_perm)

    def test_grant_table_permission(self):
        conn = self._get_mock_connection()
        conn.client.grantTablePermission = Mock()
        conn.grant_table_permission("user1", "mytable", TablePermission.BULK_IMPORT)
        conn.client.grantTablePermission.assert_called_with("Login", "user1", "mytable", TablePermission.BULK_IMPORT)

    def test_revoke_table_permission(self):
        conn = self._get_mock_connection()
        conn.client.revokeTablePermission = Mock()
        conn.revoke_table_permission("user1", "mytable", TablePermission.DROP_TABLE)
        conn.client.revokeTablePermission.assert_called_with("Login", "user1", "mytable", TablePermission.DROP_TABLE)

    def test_has_table_permission(self):
        conn = self._get_mock_connection()
        conn.client.hasTablePermission = Mock(return_value=False)
        has_perm = conn.has_table_permission("user1", "mytable", TablePermission.GRANT)
        conn.client.hasTablePermission.assert_called_with("Login", "user1", "mytable", TablePermission.GRANT)
        self.assertFalse(has_perm)

    def test_add_splits(self):
        conn = self._get_mock_connection()
        conn.client.addSplits = Mock()
        conn.add_splits("mytable", ["aa", "bb", "cc"])
        conn.client.addSplits.assert_called_with("Login", "mytable", ["aa", "bb", "cc"])

    def test_add_constraints(self):
        conn = self._get_mock_connection()
        conn.client.addConstraint = Mock(return_value=True)
        ret_val = conn.add_constraint("mytable", "org.apache.accumulo.examples.simple.constraints.MaxMutationSize")
        conn.client.addConstraint.assert_called_with(
            "Login", "mytable", "org.apache.accumulo.examples.simple.constraints.MaxMutationSize")
        self.assertTrue(ret_val)

    def test_list_constraints(self):
        conn = self._get_mock_connection()
        conn.client.listConstraints = Mock(return_value={"c1": 1, "c2": 1})
        constraints = conn.list_constraints("mytable")
        conn.client.listConstraints.assert_called_with("Login", "mytable")
        self.assertEqual(constraints, {"c1": 1, "c2": 1})

    def test_remove_constraint(self):
        conn = self._get_mock_connection()
        conn.client.removeConstraint = Mock()
        conn.remove_constraint("mytable", 1)
        conn.client.removeConstraint.assert_called_with("Login", "mytable", 1)

class RangeTest(unittest.TestCase):
    def test_to_range(self):

        r = Range(srow=None, erow=None)
        rng = r.to_range()
        self.assertEquals(None, rng.start)
        self.assertEquals(None, rng.stop)

        r = Range(srow="r01", erow="r02")
        rng = r.to_range()
        self.assertEquals(Key(row="r01"), rng.start)
        self.assertEquals(Key(row="r02\0"), rng.stop)

        r = Range(erow="r02", einclude=False)
        rng = r.to_range()
        self.assertEquals(Key(row="r02"), rng.stop)

        r = Range(srow="r01", sinclude=False)
        rng = r.to_range()
        self.assertEquals(Key(row="r01\0"), rng.start)

        r = Range(srow="r01", scf="cf1", erow="r02", ecf="cf2")
        rng = r.to_range()
        self.assertEquals(Key(row="r01", colFamily="cf1"), rng.start)
        self.assertEquals(Key(row="r02", colFamily="cf2\0"), rng.stop)

        r = Range(srow="r01", scf="cf1", scq="cq1", erow="r02", ecf="cf2", ecq="cq2")
        rng = r.to_range()
        self.assertEquals(Key(row="r01", colFamily="cf1", colQualifier="cq1"), rng.start)
        self.assertEquals(Key(row="r02", colFamily="cf2", colQualifier="cq2\0"), rng.stop)

        r = Range(srow="r01", scf="cf1", scq="cq1", erow="r02", ecf="cf2", ecq="cq2", scv="xy", ecv="zx")
        rng = r.to_range()
        self.assertEquals(Key(row="r01", colFamily="cf1", colQualifier="cq1", colVisibility="xy"), rng.start)
        self.assertEquals(Key(row="r02", colFamily="cf2", colQualifier="cq2", colVisibility="zx\0"), rng.stop)

        r = Range(srow="r01", scf="cf1", scq="cq1", erow="r02", ecf="cf2", ecq="cq2", sts=100, ets=101, scv="xy", ecv="zx")
        rng = r.to_range()
        self.assertEquals(Key(row="r01", colFamily="cf1", colQualifier="cq1", timestamp=100, colVisibility="xy"), rng.start)
        self.assertEquals(Key(row="r02", colFamily="cf2", colQualifier="cq2", timestamp=100, colVisibility="zx"), rng.stop)

        r = Range(srow="r01", scf="cf1", erow="r02", ecf="cf2", sinclude=False, einclude=False)
        rng = r.to_range()
        self.assertEquals(Key(row="r01", colFamily="cf1\0"), rng.start)
        self.assertEquals(Key(row="r02", colFamily="cf2"), rng.stop)

        r = Range(srow="r01", scf="cf1", scq="cq1", erow="r02", ecf="cf2", ecq="cq2", sinclude=False, einclude=False)
        rng = r.to_range()
        self.assertEquals(Key(row="r01", colFamily="cf1", colQualifier="cq1\0"), rng.start)
        self.assertEquals(Key(row="r02", colFamily="cf2", colQualifier="cq2"), rng.stop)

        r = Range(srow="r01", scf="cf1", scq="cq1", erow="r02", ecf="cf2", ecq="cq2", scv="xy", ecv="zx", sinclude=False, einclude=False)
        rng = r.to_range()
        self.assertEquals(Key(row="r01", colFamily="cf1", colQualifier="cq1", colVisibility="xy\0"), rng.start)
        self.assertEquals(Key(row="r02", colFamily="cf2", colQualifier="cq2", colVisibility="zx"), rng.stop)

        r = Range(srow="r01", scf="cf1", scq="cq1", erow="r02", ecf="cf2", ecq="cq2", sts=100, ets=101, scv="xy", ecv="zx", sinclude=False, einclude=False)
        rng = r.to_range()
        self.assertEquals(Key(row="r01", colFamily="cf1", colQualifier="cq1", timestamp=99, colVisibility="xy"), rng.start)
        self.assertEquals(Key(row="r02", colFamily="cf2", colQualifier="cq2", timestamp=101, colVisibility="zx"), rng.stop)

class MutationTest(unittest.TestCase):
    def test_mutation(self):
        m = Mutation("row1")
        self.assertEquals("row1", m.row)
        self.assertEquals([], m.updates)

    def test_put(self):
        m = Mutation("row1")
        self.assertEquals([], m.updates)
        m.put(cf="cf1", cq="cq1", cv="xy", ts=101, val="myval")
        self.assertEquals([ColumnUpdate(colFamily="cf1", colQualifier="cq1", colVisibility="xy", timestamp=101, value="myval", deleteCell=None)], m.updates)

        
class BatchWriterTest(unittest.TestCase):
    def test_init(self):
        conn = Accumulo(_connect=False)
        conn.client = Mock()
        conn.client.createWriter = Mock(return_value="writer1")
        conn.login = "Login"

        b = BatchWriter(conn=conn, table="mytable", max_memory=10, latency_ms=30, timeout_ms=5, threads=11)
        conn.client.createWriter.assert_called_with("Login", "mytable", WriterOptions(maxMemory=10, latencyMs=30, timeoutMs=5, threads=11))

        #-----

        conn.client.update = Mock()
        b._writer = "writer1"
        mut = Mock(row="r01", updates=["x", "y", "z"])
        b.add_mutation(mut)
        conn.client.update.assert_called_with("writer1", {"r01": ["x", "y", "z"]})

        # ----

        conn.client.update = Mock()
        b._writer = "writer1"
        mut1 = Mock(row="r01", updates=["x", "y", "z"])
        mut2 = Mock(row="r02", updates=["a", "b", "c"])
        b.add_mutations([mut1, mut2])
        conn.client.update.assert_called_with("writer1", {"r01": ["x", "y", "z"], "r02":["a", "b", "c"]})

        # ----
        conn.client.flush = Mock()
        b.flush()
        conn.client.flush.assert_called_with("writer1")

        # ----
        conn.client.closeWriter = Mock()
        b.close()
        conn.client.closeWriter.assert_called_with("writer1")
        self.assertTrue(b._is_closed)

        #----
        # Writer is now closed, so...

        with self.assertRaises(Exception):
            b.add_mutation(mut)

        with self.assertRaises(Exception):
            b.add_mutations([mut1, mut2])

        with self.assertRaises(Exception):
            b.flush()


    def test_close(self):
        pass

    def test_flush(self):
        pass

#----------------------------------------------

def main():
    unittest.main()

if __name__ == '__main__':
    main()
