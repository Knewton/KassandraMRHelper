/**
 * Copyright 2013, 2014, 2015 Knewton
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
package com.knewton.mapreduce;

import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.DeletedColumn;
import org.apache.cassandra.db.IColumn;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Tests the {@link SSTableColumnMapper} through its test implementation the
 * {@link DoNothingColumnMapper}.
 *
 * @author Giannis Neokleous
 *
 */
public class SSTableColumnMapperTest {

    private DoNothingColumnMapper underTest;
    private String keyVal;
    private String columnNameVal;
    private String columnValue;

    @Before
    public void setUp() throws Exception {
        this.underTest = new DoNothingColumnMapper();
        this.keyVal = "testKey";
        this.columnNameVal = "testColumnName";
        this.columnValue = "testColumnValue";
    }

    /**
     * Test to see if cassandra columns go through the mapper correctly.
     */
    @Test
    public void testMap() throws Exception {

        ByteBuffer key = ByteBuffer.wrap(keyVal.getBytes());
        IColumn column = new Column(ByteBuffer.wrap(columnNameVal.getBytes()),
                                    ByteBuffer.wrap(columnValue.getBytes()));
        underTest.map(key, column, null);
        // Key
        byte[] verifyByteArr = new byte[keyVal.length()];
        underTest.getCurrentKey().get(verifyByteArr);
        assertEquals(keyVal, new String(verifyByteArr));
        // Column name
        verifyByteArr = new byte[columnNameVal.length()];
        underTest.getCurrentColumn().name().get(verifyByteArr);
        assertEquals(columnNameVal, new String(verifyByteArr));
        // Column value
        verifyByteArr = new byte[columnValue.length()];
        underTest.getCurrentColumn().value().get(verifyByteArr);
        assertEquals(columnValue, new String(verifyByteArr));
        underTest.clearKeyVal();
    }

    /**
     * Tests to see if deleted columns don't flow through the mapper.
     */
    @Test
    public void testMapDeletedColumn() throws Exception {
        underTest.setSkipDeletedColumns(true);
        IColumn column = new DeletedColumn(ByteBuffer.wrap(columnNameVal.getBytes()),
                                           ByteBuffer.wrap(columnValue.getBytes()),
                                           System.currentTimeMillis() - 100);
        underTest.map(ByteBuffer.wrap(keyVal.getBytes()), column, null);
        assertNull(underTest.getCurrentKey());
        assertNull(underTest.getCurrentColumn());
        underTest.clearKeyVal();
    }

    /**
     * Tests to see that null keys and values do not make the mapper fail
     */
    @Test
    public void testMapWithNulls() throws Exception {
        underTest.map(null, null, null);
        assertNull(underTest.getCurrentKey());
        assertNull(underTest.getRowKey());
        assertNull(underTest.getCurrentColumn());

        ByteBuffer key = ByteBuffer.wrap("testKey".getBytes());
        underTest.map(key, null, null);
        assertNull(underTest.getCurrentKey());
        assertNull(underTest.getRowKey());
        assertNull(underTest.getCurrentColumn());

        IColumn mockColumn = mock(IColumn.class);
        underTest.map(null, mockColumn, null);
        assertNull(underTest.getCurrentKey());
        assertNull(underTest.getRowKey());
        assertNull(underTest.getCurrentColumn());
    }

    /**
     * Tests the skip deleted columns option
     */
    @Test
    public void testSetSkipDeletedColumns() {
        underTest.setSkipDeletedColumns(true);
        assertTrue(underTest.isSkipDeletedColumns());
        underTest.setSkipDeletedColumns(false);
        assertTrue(!underTest.isSkipDeletedColumns());
    }
}
