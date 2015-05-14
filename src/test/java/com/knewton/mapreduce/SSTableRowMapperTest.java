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

import org.apache.cassandra.io.sstable.SSTableIdentityIterator;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;

/**
 * Tests the {@link SSTableRowMapper} through a do-nothing implementation, the
 * {@link DoNothingRowMapper}.
 *
 * @author Giannis Neokleous
 *
 */
public class SSTableRowMapperTest {

    private DoNothingRowMapper underTest;

    @Before
    public void setUp() throws Exception {
        this.underTest = new DoNothingRowMapper();
    }

    /**
     * Makes sure that keys and values are properly set
     */
    @Test
    public void testMap() throws Exception {
        ByteBuffer key = ByteBuffer.wrap("testKey".getBytes());
        SSTableIdentityIterator mockRowIter = mock(SSTableIdentityIterator.class);
        underTest.map(key, mockRowIter, null);
        assertEquals(key, underTest.getCurrentKey());
        assertEquals(mockRowIter, underTest.getCurrentRow());
        assertEquals(key, underTest.getRowKey());
        assertEquals(mockRowIter, underTest.getRowIterator());
    }

    /**
     * Tests to see that null keys and values do not make the mapper fail
     */
    @Test
    public void testMapWithNulls() throws Exception {
        underTest.map(null, null, null);
        assertNull(underTest.getCurrentKey());
        assertNull(underTest.getRowKey());
        assertNull(underTest.getCurrentRow());

        ByteBuffer key = ByteBuffer.wrap("testKey".getBytes());
        underTest.map(key, null, null);
        assertNull(underTest.getCurrentKey());
        assertNull(underTest.getRowKey());
        assertNull(underTest.getCurrentRow());

        SSTableIdentityIterator mockRowIter = mock(SSTableIdentityIterator.class);
        underTest.map(null, mockRowIter, null);
        assertNull(underTest.getCurrentKey());
        assertNull(underTest.getRowKey());
        assertNull(underTest.getCurrentRow());
    }

}
