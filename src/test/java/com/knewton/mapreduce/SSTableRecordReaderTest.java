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
import com.knewton.mapreduce.io.SSTableInputFormat;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.sstable.SSTableScanner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.File;
import java.io.IOException;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests {@link SSTableRecordReader}
 *
 * @author Giannis Neokleous
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class SSTableRecordReaderTest {
    private static final String TABLE_PATH_STR = "keyspace/columnFamily";

    @Mock
    private SSTableReader ssTableReader;

    @Mock
    private SSTableScanner tableScanner;

    @Spy
    private SSTableColumnRecordReader ssTableColumnRecordReader;

    @Spy
    private SSTableRowRecordReader ssTableRowRecordReader;

    private FileSplit inputSplit;
    private TaskAttemptID attemptId;
    private Configuration conf;
    private Job job;

    @Before
    public void setup() throws IOException {
        job = Job.getInstance();
        conf = job.getConfiguration();
        attemptId = new TaskAttemptID();
        Path inputPath = new Path(TABLE_PATH_STR);
        inputSplit = new FileSplit(inputPath, 0, 1, null);
        Descriptor desc = new Descriptor(new File(TABLE_PATH_STR), "keyspace", "columnFamily", 1,
                                         false);

        doReturn(desc).when(ssTableColumnRecordReader).getDescriptor();
        doReturn(desc).when(ssTableRowRecordReader).getDescriptor();

        doNothing().when(ssTableColumnRecordReader).copyTablesToLocal(any(FileSplit.class),
                                                                     any(TaskAttemptContext.class));
        doNothing().when(ssTableRowRecordReader).copyTablesToLocal(any(FileSplit.class),
                                                                   any(TaskAttemptContext.class));

        doReturn(ssTableReader).when(ssTableColumnRecordReader)
            .openSSTableReader(any(IPartitioner.class), any(CFMetaData.class));
        doReturn(ssTableReader).when(ssTableRowRecordReader)
            .openSSTableReader(any(IPartitioner.class), any(CFMetaData.class));
        when(ssTableReader.getDirectScanner(null)).thenReturn(tableScanner);
    }

    private TaskAttemptContext getTaskAttemptContext(boolean setColumnComparator,
                                                     boolean setPartitioner,
                                                     boolean setSubComparator) throws Exception {

        if (setColumnComparator) {
            SSTableInputFormat.setComparatorClass(LongType.class.getName(), job);
        }
        if (setPartitioner) {
            SSTableInputFormat.setPartitionerClass(RandomPartitioner.class.getName(), job);
        }
        if (setSubComparator) {
            SSTableInputFormat.setSubComparatorClass(LongType.class.getName(), job);
        }

        return new TaskAttemptContextImpl(conf, attemptId);
    }

    /**
     * Test a valid configuration of the SSTableColumnRecordReader.
     */
    @Test
    public void testInitializeColumnReader() throws Exception {
        TaskAttemptContext context = getTaskAttemptContext(true, true, false);
        ssTableColumnRecordReader.initialize(inputSplit, context);
        verify(ssTableColumnRecordReader).copyTablesToLocal(inputSplit, context);
        ssTableColumnRecordReader.close();
    }

    /**
     * Test a valid configuration of the SSTableRowRecordReader.
     */
    @Test
    public void testInitializeRowReader() throws Exception {
        TaskAttemptContext context = getTaskAttemptContext(true, true, true);
        ssTableRowRecordReader.initialize(inputSplit, context);
        verify(ssTableRowRecordReader).copyTablesToLocal(inputSplit, context);
        ssTableRowRecordReader.close();
    }

    /**
     * Test for validating to make sure that a partitioner must be set.
     */
    @Test(expected = NullPointerException.class)
    public void testInitializeNoPartitioner() throws Exception {
        TaskAttemptContext context = getTaskAttemptContext(true, false, false);
        ssTableColumnRecordReader.initialize(inputSplit, context);
    }

    /**
     * Test for validating to make sure that a column comparator must be set.
     */
    @Test(expected = NullPointerException.class)
    public void testInitializeNoComparator() throws Exception {
        TaskAttemptContext context = getTaskAttemptContext(false, true, false);
        ssTableColumnRecordReader.initialize(inputSplit, context);
    }

    /**
     * Test to make sure that initialization fails when an invalid partitioner is set.
     */
    @Test (expected = IllegalArgumentException.class)
    public void testInitializeInvalidPartitioner() throws Exception {
        TaskAttemptContext context = getTaskAttemptContext(true, true, true);
        context.getConfiguration().set(PropertyConstants.PARTITIONER.txt,
                                       "invalidPartitioner");
        ssTableColumnRecordReader.initialize(inputSplit, context);
    }

    /**
     * Test to make sure that initialization fails when an invalid comparator is set.
     */
    @Test (expected = IllegalArgumentException.class)
    public void testInitializeInvalidComparator() throws Exception {
        TaskAttemptContext context = getTaskAttemptContext(true, true, true);
        context.getConfiguration().set(PropertyConstants.COLUMN_COMPARATOR.txt,
                                       "invalidComparator");
        ssTableColumnRecordReader.initialize(inputSplit, context);
    }
}
