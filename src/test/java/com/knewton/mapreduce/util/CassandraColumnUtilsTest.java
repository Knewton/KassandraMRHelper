/**
 * Copyright 2013 Knewton
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */
package com.knewton.mapreduce.util;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.db.CounterColumn;
import org.apache.cassandra.db.CounterUpdateColumn;
import org.apache.cassandra.db.DeletedColumn;
import org.apache.cassandra.db.ExpiringColumn;
import org.apache.cassandra.db.SuperColumn;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.io.IColumnSerializer;
import org.apache.cassandra.io.sstable.SSTableIdentityIterator;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CassandraColumnUtilsTest {

    /**
     * Test if an <code>IColumn</code> is a super column.
     */
    @Test
    public void testCheckForSuperColumn() throws Exception {
        SuperColumn sc = new SuperColumn(getDummyByteBuffer("name"), null);
        assertTrue(CassandraColumnUtils.isSuperColumn(sc));

        Column column = new Column(getDummyByteBuffer("name"), getDummyByteBuffer("value"));
        assertFalse(CassandraColumnUtils.isSuperColumn(column));

        column =  new CounterColumn(getDummyByteBuffer("name"), getDummyByteBuffer("value"), 0);
        assertFalse(CassandraColumnUtils.isSuperColumn(column));

        column = new CounterUpdateColumn(getDummyByteBuffer("name"),
                                         getDummyByteBuffer("value"), 0);
        assertFalse(CassandraColumnUtils.isSuperColumn(column));

        column = new DeletedColumn(getDummyByteBuffer("name"), getDummyByteBuffer("value"), 0);
        assertFalse(CassandraColumnUtils.isSuperColumn(column));

        column = new ExpiringColumn(getDummyByteBuffer("name"),
                                    getDummyByteBuffer("value"), 0, 100);
        assertFalse(CassandraColumnUtils.isSuperColumn(column));
    }

    /**
     * Test if sstable has super columns with SSTableIdentityIterator
     */
    @Test
    public void testCheckForSuperColumnWithIterator() throws Exception {

        DataInput in = new DataInputStream(new ByteArrayInputStream(new byte[32]));
        CFMetaData metadata = new CFMetaData("keyspace",
                                             "columnFamily",
                                             ColumnFamilyType.Standard,
                                             TypeParser.parse(LongType.class.getName()),
                                             null);
        SSTableIdentityIterator iter = new SSTableIdentityIterator(metadata, in, "filename", null,
                                                                   0, 1,
                                                                   IColumnSerializer.Flag.LOCAL);
        assertFalse(CassandraColumnUtils.isSuperColumn(iter));

        in = new DataInputStream(new ByteArrayInputStream(new byte[32]));
        metadata = new CFMetaData("keyspace",
                                  "columnFamily",
                                  ColumnFamilyType.Super,
                                  TypeParser.parse(LongType.class.getName()),
                                  null);
        iter = new SSTableIdentityIterator(metadata, in, "filename", null, 0, 1,
                                           IColumnSerializer.Flag.LOCAL);
        assertTrue(CassandraColumnUtils.isSuperColumn(iter));
        iter.close();
    }

    private ByteBuffer getDummyByteBuffer(String value) {
        return ByteBuffer.wrap(value.getBytes());
    }

}
