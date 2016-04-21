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

import org.apache.cassandra.db.BufferCell;
import org.apache.cassandra.db.BufferDeletedCell;
import org.apache.cassandra.db.Cell;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.SimpleDenseCellNameType;
import org.apache.cassandra.db.marshal.BytesType;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests the {@link SSTableColumnMapper} through its test implementation the
 * {@link DoNothingColumnMapper}.
 *
 * @author Giannis Neokleous
 *
 */
public class SSTableCellMapperTest {

    private DoNothingColumnMapper underTest;
    private String keyVal;
    private String columnNameVal;
    private String columnValue;
    private SimpleDenseCellNameType simpleDenseCellType;

    @Before
    public void setUp() throws Exception {
        this.underTest = new DoNothingColumnMapper();
        this.keyVal = "testKey";
        this.columnNameVal = "testColumnName";
        this.columnValue = "testColumnValue";
        this.simpleDenseCellType = new SimpleDenseCellNameType(BytesType.instance);
    }

    /**
     * Test to see if cassandra columns go through the mapper correctly.
     */
    @Test
    public void testMap() throws Exception {
        ByteBuffer columnNameBytes = ByteBuffer.wrap(columnNameVal.getBytes());
        CellName cellName = simpleDenseCellType.cellFromByteBuffer(columnNameBytes);

        ByteBuffer key = ByteBuffer.wrap(keyVal.getBytes());
        Cell column = new BufferCell(cellName, ByteBuffer.wrap(columnValue.getBytes()));

        underTest.map(key, column, null);
        // Key
        byte[] verifyByteArr = new byte[keyVal.length()];
        underTest.getCurrentKey().get(verifyByteArr);
        assertEquals(keyVal, new String(verifyByteArr));
        // Column name
        verifyByteArr = new byte[columnNameVal.length()];
        underTest.getCurrentCell().name().toByteBuffer().get(verifyByteArr);
        assertEquals(columnNameVal, new String(verifyByteArr));
        // Column value
        verifyByteArr = new byte[columnValue.length()];
        underTest.getCurrentCell().value().get(verifyByteArr);
        assertEquals(columnValue, new String(verifyByteArr));
        underTest.clearKeyVal();
    }

    /**
     * Tests to see if deleted columns don't flow through the mapper.
     */
    @Test
    public void testMapDeletedColumn() throws Exception {
        underTest.setSkipDeletedAtoms(true);
        ByteBuffer columnNameBytes = ByteBuffer.wrap(columnNameVal.getBytes());
        CellName cellName = simpleDenseCellType.cellFromByteBuffer(columnNameBytes);

        Cell column = new BufferDeletedCell(cellName,
                                                  ByteBuffer.wrap(columnValue.getBytes()),
                                                  System.currentTimeMillis() - 100);
        underTest.map(ByteBuffer.wrap(keyVal.getBytes()), column, null);
        assertNull(underTest.getCurrentKey());
        assertNull(underTest.getCurrentCell());
        underTest.clearKeyVal();
    }

    /**
     * Tests the skip deleted columns option
     */
    @Test
    public void testSetSkipDeletedColumns() {
        underTest.setSkipDeletedAtoms(true);
        assertTrue(underTest.isSkipDeletedAtoms());
        underTest.setSkipDeletedAtoms(false);
        assertTrue(!underTest.isSkipDeletedAtoms());
    }
}
