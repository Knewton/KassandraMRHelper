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

import static org.junit.Assert.*;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.io.IColumnSerializer;
import org.apache.cassandra.io.sstable.SSTableIdentityIterator;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class CassandraColumnUtilsTest {

    /**
     * Test if an <code>IColumn</code> is a super column.
     * 
     * @throws ConfigurationException
     */
    @Test
    public void testCheckForSuperColumn() throws ConfigurationException {
        SuperColumn sc = new SuperColumn(getDummyByteBuffer("name"), null);
        assertTrue(CassandraColumnUtils.isSuperColumn(sc));
        assertFalse(CassandraColumnUtils.isSuperColumn(
                new Column(getDummyByteBuffer("name"),
                        getDummyByteBuffer("value"))));
        assertFalse(CassandraColumnUtils.isSuperColumn(
                new CounterColumn(getDummyByteBuffer("name"),
                        getDummyByteBuffer("value"), 0)));
        assertFalse(CassandraColumnUtils.isSuperColumn(
                new CounterUpdateColumn(getDummyByteBuffer("name"),
                        getDummyByteBuffer("value"), 0)));
        assertFalse(CassandraColumnUtils.isSuperColumn(
                new DeletedColumn(getDummyByteBuffer("name"),
                        getDummyByteBuffer("value"), 0)));
        assertFalse(CassandraColumnUtils.isSuperColumn(
                new ExpiringColumn(getDummyByteBuffer("name"),
                        getDummyByteBuffer("value"), 0, 100)));
    }

    /**
     * Test if sstable has super columns with SSTableIdentityIterator
     * 
     * @throws ConfigurationException
     * @throws IOException
     * @throws SyntaxException
     */
    @Test
    public void testCheckForSuperColumnWithIterator() throws
            ConfigurationException, IOException, SyntaxException {

        DataInput in = new DataInputStream(
                new ByteArrayInputStream(new byte[32]));
        CFMetaData metadata = new CFMetaData(
                "keyspace",
                "columnFamily",
                ColumnFamilyType.Standard,
                TypeParser.parse(LongType.class.getName()),
                null);
        SSTableIdentityIterator iter = new SSTableIdentityIterator(
                metadata, in, "filename", null, 0, 1, IColumnSerializer.Flag.LOCAL);
        assertFalse(CassandraColumnUtils.isSuperColumn(iter));

        in = new DataInputStream(
                new ByteArrayInputStream(new byte[32]));
        metadata = new CFMetaData(
                "keyspace",
                "columnFamily",
                ColumnFamilyType.Super,
                TypeParser.parse(LongType.class.getName()),
                null);
        iter = new SSTableIdentityIterator(
                metadata, in, "filename", null, 0, 1, IColumnSerializer.Flag.LOCAL);
        assertTrue(CassandraColumnUtils.isSuperColumn(iter));
    }

    private ByteBuffer getDummyByteBuffer(String value) {
        return ByteBuffer.wrap(value.getBytes());
    }

}
