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

import com.knewton.mapreduce.constant.PropertyConstants;

import org.apache.cassandra.config.CFMetaData;
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
 * Tests the {@link SSTableRowRecordReader}. This test sets up an sstable scanner with a single row
 * and makes sure that it can be read by the record reader.
 *
 * @author Giannis Neokleous
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class SSTableRowRecordReaderTest {
    private static final String TABLE_PATH_STR = "keyspace/columnFamily";

    @Mock
    private SSTableReader ssTableReader;

    @Mock
    private SSTableScanner tableScanner;

    @Spy
    private SSTableRowRecordReader ssTableRowRecordReader;

    private DecoratedKey key;
    private FileSplit inputSplit;
    private TaskAttemptID attemptId;
    private Configuration conf;

    private SSTableIdentityIterator row;

    @Before
    public void setUp() throws Exception {
        conf = new Configuration();
        attemptId = new TaskAttemptID();
        Path inputPath = new Path(TABLE_PATH_STR);
        inputSplit = new FileSplit(inputPath, 0, 1, null);
        Descriptor desc = new Descriptor(new File(TABLE_PATH_STR), "keyspace", "columnFamily", 1,
                                         false);

        doReturn(desc).when(ssTableRowRecordReader).getDescriptor();

        doNothing().when(ssTableRowRecordReader).copyTablesToLocal(any(FileSplit.class),
                                                                   any(TaskAttemptContext.class));

        doReturn(ssTableReader).when(ssTableRowRecordReader)
            .openSSTableReader(any(IPartitioner.class), any(CFMetaData.class));

        when(ssTableReader.estimatedKeys()).thenReturn(1L);
        when(ssTableReader.getDirectScanner(null)).thenReturn(tableScanner);

        when(tableScanner.hasNext()).thenReturn(true, false);

        key = new DecoratedKey(new StringToken("a"), ByteBuffer.wrap("b".getBytes()));

        row = mock(SSTableIdentityIterator.class);
        when(row.getKey()).thenReturn(key);

        when(tableScanner.next()).thenReturn(row);
    }

    private TaskAttemptContext getTaskAttemptContext() {
        conf.set(PropertyConstants.COLUMN_COMPARATOR.txt, LongType.class.getName());
        conf.set(PropertyConstants.PARTITIONER.txt,
                 RandomPartitioner.class.getName());
        return new TaskAttemptContextImpl(conf, attemptId);
    }

    /**
     * Make sure that the single row that was set can be read and returned
     */
    @Test
    public void testNextKeyValue() throws Exception {
        TaskAttemptContext context = getTaskAttemptContext();
        ssTableRowRecordReader.initialize(inputSplit, context);
        verify(ssTableRowRecordReader).copyTablesToLocal(inputSplit, context);

        assertEquals(0, ssTableRowRecordReader.getProgress(), 0);
        assertTrue(ssTableRowRecordReader.nextKeyValue());
        assertEquals(key.key, ssTableRowRecordReader.getCurrentKey());
        assertEquals(row, ssTableRowRecordReader.getCurrentValue());

        assertEquals(1, ssTableRowRecordReader.getProgress(), 0);
        assertFalse(ssTableRowRecordReader.nextKeyValue());
        assertNull(ssTableRowRecordReader.getCurrentKey());
        assertNull(ssTableRowRecordReader.getCurrentValue());
    }

    @After
    public void tearDown() throws Exception {
        ssTableRowRecordReader.close();
    }

}
