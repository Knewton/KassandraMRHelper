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
package com.knewton.mapreduce.util;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.io.sstable.SSTableIdentityIterator;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CassandraColumnUtilsTest {

    /**
     * Test if sstable has super columns with SSTableIdentityIterator
     */
    @Test
    public void testCheckForSuperColumnWithIterator() throws Exception {

        SSTableIdentityIterator iter = mock(SSTableIdentityIterator.class);
        ColumnFamily cf = mock(ColumnFamily.class);

        when(iter.getColumnFamily()).thenReturn(cf);
        when(cf.getType()).thenReturn(ColumnFamilyType.Standard);

        assertFalse(CassandraColumnUtils.isSuperColumn(iter));

        when(cf.getType()).thenReturn(ColumnFamilyType.Super);

        assertTrue(CassandraColumnUtils.isSuperColumn(iter));
    }

}
