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
package com.knewton.mapreduce;

import com.knewton.mapreduce.constant.PropertyConstants;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.CounterColumn;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.dht.StringToken;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableIdentityIterator;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.sstable.SSTableScanner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.File;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * This tests {@link SSTableColumnRecordReader}
 *
 * @author Giannis Neokleous
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class SSTableColumnRecordReaderTest {

    private static final String TABLE_PATH_STR = "keyspace/columnFamily";

    @Mock
    private SSTableReader ssTableReader;

    @Mock
    private SSTableScanner tableScanner;

    @Spy
    private SSTableColumnRecordReader ssTableColumnRecordReader;

    private DecoratedKey key;
    private CounterColumn value;
    private FileSplit inputSplit;
    private TaskAttemptID attemptId;
    private Configuration conf;

    @Before
    public void setUp() throws Exception {
        conf = new Configuration();
        attemptId = new TaskAttemptID();
        Path inputPath = new Path(TABLE_PATH_STR);
        inputSplit = new FileSplit(inputPath, 0, 1, null);
        Descriptor desc = new Descriptor(new File(TABLE_PATH_STR), "keyspace", "columnFamily", 1,
                                         false);

        doReturn(desc).when(ssTableColumnRecordReader).getDescriptor();

        doNothing().when(ssTableColumnRecordReader).copyTablesToLocal(any(FileSplit.class),
                                                                     any(TaskAttemptContext.class));

        doReturn(ssTableReader).when(ssTableColumnRecordReader)
            .openSSTableReader(any(IPartitioner.class), any(CFMetaData.class));

        when(ssTableReader.estimatedKeys()).thenReturn(2L);
        when(ssTableReader.getDirectScanner(null)).thenReturn(tableScanner);

        when(tableScanner.hasNext()).thenReturn(true, true, false);

        key = new DecoratedKey(new StringToken("a"), ByteBuffer.wrap("b".getBytes()));
        value = new CounterColumn(ByteBuffer.wrap("name".getBytes()), 1L, 0L);

        SSTableIdentityIterator row1 = getNewRow();
        SSTableIdentityIterator row2 = getNewRow();

        when(tableScanner.next()).thenReturn(row1, row2);
    }

    private SSTableIdentityIterator getNewRow() {
        SSTableIdentityIterator row = mock(SSTableIdentityIterator.class);
        when(row.hasNext()).thenReturn(true, true, false);
        when(row.getKey()).thenReturn(key);
        when(row.next()).thenReturn(value);
        return row;
    }

    private TaskAttemptContext getTaskAttemptContext() {
        conf.set(PropertyConstants.COLUMN_COMPARATOR.txt, LongType.class.getName());
        conf.set(PropertyConstants.PARTITIONER.txt,
                 RandomPartitioner.class.getName());
        return new TaskAttemptContextImpl(conf, attemptId);
    }

    @Test
    public void testNextKeyValue() throws Exception {
        TaskAttemptContext context = getTaskAttemptContext();
        ssTableColumnRecordReader.initialize(inputSplit, context);
        verify(ssTableColumnRecordReader).copyTablesToLocal(inputSplit, context);

        assertEquals(0, ssTableColumnRecordReader.getProgress(), 0);
        assertTrue(ssTableColumnRecordReader.nextKeyValue());
        assertEquals(key.key, ssTableColumnRecordReader.getCurrentKey());
        assertEquals(value, ssTableColumnRecordReader.getCurrentValue());

        assertEquals(0.5, ssTableColumnRecordReader.getProgress(), 0);
        assertTrue(ssTableColumnRecordReader.nextKeyValue());
        assertEquals(key.key, ssTableColumnRecordReader.getCurrentKey());
        assertEquals(value, ssTableColumnRecordReader.getCurrentValue());

        assertEquals(1, ssTableColumnRecordReader.getProgress(), 0);
        assertFalse(ssTableColumnRecordReader.nextKeyValue());
        assertNull(ssTableColumnRecordReader.getCurrentKey());
        assertNull(ssTableColumnRecordReader.getCurrentValue());
    }

    @After
    public void tearDown() throws Exception {
        ssTableColumnRecordReader.close();
    }

}
