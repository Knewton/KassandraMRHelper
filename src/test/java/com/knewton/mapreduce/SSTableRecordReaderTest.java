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

import static org.junit.Assert.*;

import mockit.*;

import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class SSTableRecordReaderTest {

    /**
     * Test a valid configuration of the SSTableColumnRecordReader.
     * 
     * @param mock
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void testValidateConfigurationColumnReader(
            @Injectable final SSTableReader mock)
            throws IOException, InterruptedException {
        final String tablePathStr = "keyspace/columnFamily";
        new NonStrictExpectations() {
            @Mocked
            SSTableReader tableReader;
            {
                SSTableReader.open(null, null, null, null);
                returns(null);
            }
        };
        new MockUp<SSTableColumnRecordReader>() {
            Descriptor desc = new Descriptor(new File(tablePathStr),
                    "keyspace", "columnFamily", 1, false);

            @Mock
            void copyTablesToLocal(FileSplit split, TaskAttemptContext ctx) {
                assertEquals(split.getPath().toString(), tablePathStr);
            }

            @Mock
            public Descriptor getDescriptor() {
                return desc;
            }

            @Mock
            // No op
            private void setTableScanner() {
            }
        };
        SSTableColumnRecordReader rr = new SSTableColumnRecordReader();
        Configuration conf = new Configuration();
        TaskAttemptID attemptID = new TaskAttemptID();
        Path inputPath = new Path(tablePathStr);
        FileSplit inputSplit = new FileSplit(inputPath, 0, 1, null);
        conf.set(SSTableRecordReader.COLUMN_COMPARATOR_PARAMETER,
                LongType.class.getName());
        conf.set(SSTableRecordReader.PARTITIONER_PARAMETER,
                RandomPartitioner.class.getName());
        TaskAttemptContext context = new TaskAttemptContext(conf, attemptID);

        rr.initialize(inputSplit, context);
    }

    /**
     * Test a valid configuration of the SSTableRowRecordReader.
     * 
     * @param mock
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void testValidateConfigurationRowReader(
            @Injectable final SSTableReader mock)
            throws IOException, InterruptedException {
        final String tablePathStr = "keyspace/columnFamily";
        new NonStrictExpectations() {
            @Mocked
            SSTableReader tableReader;
            {
                SSTableReader.open(null, null, null, null);
                returns(null);
            }
        };
        new MockUp<SSTableRowRecordReader>() {
            Descriptor desc = new Descriptor(new File(tablePathStr),
                    "keyspace", "columnFamily", 1, false);

            @Mock
            void copyTablesToLocal(FileSplit split, TaskAttemptContext ctx) {
                assertEquals(split.getPath().toString(), tablePathStr);
            }

            @Mock
            public Descriptor getDescriptor() {
                return desc;
            }

            @Mock
            private void setTableScanner() {
                // no op.
            }
        };
        SSTableRowRecordReader rr = new SSTableRowRecordReader();
        Configuration conf = new Configuration();
        TaskAttemptID attemptID = new TaskAttemptID();
        Path inputPath = new Path(tablePathStr);
        FileSplit inputSplit = new FileSplit(inputPath, 0, 1, null);
        conf.set(SSTableRecordReader.COLUMN_COMPARATOR_PARAMETER,
                LongType.class.getName());
        conf.set(SSTableRecordReader.PARTITIONER_PARAMETER,
                RandomPartitioner.class.getName());
        TaskAttemptContext context = new TaskAttemptContext(conf, attemptID);

        rr.initialize(inputSplit, context);
    }

    /**
     * Test for validating to make sure that a partitioner must be set.
     * 
     * @param mock
     * @throws IOException
     * @throws InterruptedException
     */
    @Test(expected = NullPointerException.class)
    public void testValidateFailingConfigurationPartitioner(
            @Injectable final SSTableReader mock)
            throws IOException, InterruptedException {
        final String tablePathStr = "keyspace/columnFamily";
        new NonStrictExpectations() {
            @Mocked
            SSTableReader tableReader;
            {
                SSTableReader.open(null, null, null, null);
                returns(null);
            }
        };
        SSTableColumnRecordReader rr = new SSTableColumnRecordReader();
        Configuration conf = new Configuration();
        TaskAttemptID attemptID = new TaskAttemptID();
        Path inputPath = new Path(tablePathStr);
        FileSplit inputSplit = new FileSplit(inputPath, 0, 1, null);
        conf.set(SSTableRecordReader.COLUMN_COMPARATOR_PARAMETER,
                LongType.class.getName());
        TaskAttemptContext context = new TaskAttemptContext(conf, attemptID);
        rr.initialize(inputSplit, context);
    }

    /**
     * Test for validating to make sure that a column comparator must be set.
     * 
     * @param mock
     * @throws IOException
     * @throws InterruptedException
     */
    @Test(expected = NullPointerException.class)
    public void testValidateFailingConfigurationComparator(
            @Injectable final SSTableReader mock)
            throws IOException, InterruptedException {
        final String tablePathStr = "keyspace/columnFamily";
        new NonStrictExpectations() {
            @Mocked
            SSTableReader tableReader;
            {
                SSTableReader.open(null, null, null, null);
                returns(null);
            }
        };
        SSTableColumnRecordReader rr = new SSTableColumnRecordReader();
        Configuration conf = new Configuration();
        TaskAttemptID attemptID = new TaskAttemptID();
        Path inputPath = new Path(tablePathStr);
        FileSplit inputSplit = new FileSplit(inputPath, 0, 1, null);
        conf.set(SSTableRecordReader.PARTITIONER_PARAMETER,
                RandomPartitioner.class.getName());
        TaskAttemptContext context = new TaskAttemptContext(conf, attemptID);
        rr.initialize(inputSplit, context);
    }

}
