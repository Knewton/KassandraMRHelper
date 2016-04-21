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
import com.knewton.mapreduce.io.MemoryDataInputStream;
import com.knewton.mapreduce.io.SSTableInputFormat;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
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

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests {@link SSTableRecordReader}.
 *
 * @author Giannis Neokleous
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class SSTableRecordReaderTest {

    private static final String KEYSPACE_NAME = "keyspace";
    private static final String CF_NAME = "columnfamily";

    private static final String TABLE_PATH_STR =
            String.format("%s/%s/%s-%s-hg-1-Data.db",
                          KEYSPACE_NAME, CF_NAME, KEYSPACE_NAME, CF_NAME);

    @Mock
    private SSTableReader ssTableReader;

    @Mock
    private ISSTableScanner tableScanner;

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
        conf.setInt("mapreduce.task.attempt.id", attemptId.getId());
        conf.set("mapreduce.cluster.temp.dir", "tempdir");

        Path inputPath = new Path(TABLE_PATH_STR);
        inputSplit = new FileSplit(inputPath, 0, 1, null);
        Descriptor desc = Descriptor.fromFilename(TABLE_PATH_STR);

        doReturn(desc).when(ssTableColumnRecordReader).getDescriptor();
        doReturn(desc).when(ssTableRowRecordReader).getDescriptor();

        doNothing().when(ssTableColumnRecordReader).copyTablesToLocal(any(FileSystem.class),
                                                                      any(FileSystem.class),
                                                                      any(Path.class),
                                                                      any(TaskAttemptContext.class));
        doNothing().when(ssTableRowRecordReader).copyTablesToLocal(any(FileSystem.class),
                                                                   any(FileSystem.class),
                                                                   any(Path.class),
                                                                   any(TaskAttemptContext.class));

        doReturn(ssTableReader).when(ssTableColumnRecordReader)
            .openSSTableReader(any(IPartitioner.class), any(CFMetaData.class));
        doReturn(ssTableReader).when(ssTableRowRecordReader)
            .openSSTableReader(any(IPartitioner.class), any(CFMetaData.class));
        when(ssTableReader.getScanner()).thenReturn(tableScanner);
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
        Path inputPath = inputSplit.getPath();
        FileSystem remoteFS = FileSystem.get(inputPath.toUri(), conf);
        FileSystem localFS = FileSystem.getLocal(conf);
        TaskAttemptContext context = getTaskAttemptContext(true, true, false);
        ssTableColumnRecordReader.initialize(inputSplit, context);
        verify(ssTableColumnRecordReader).copyTablesToLocal(remoteFS, localFS, inputPath, context);
        ssTableColumnRecordReader.close();
    }

    /**
     * Test a valid configuration of the SSTableRowRecordReader.
     */
    @Test
    public void testInitializeRowReader() throws Exception {
        Path inputPath = inputSplit.getPath();
        FileSystem remoteFS = FileSystem.get(inputPath.toUri(), conf);
        FileSystem localFS = FileSystem.getLocal(conf);
        TaskAttemptContext context = getTaskAttemptContext(true, true, true);
        ssTableRowRecordReader.initialize(inputSplit, context);
        verify(ssTableRowRecordReader).copyTablesToLocal(remoteFS, localFS, inputPath, context);
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

    /**
     * Test to make sure that initialization fails when an invalid subcomparator is set.
     */
    @Test (expected = IllegalArgumentException.class)
    public void testInitializeInvalidSubComparator() throws Exception {
        TaskAttemptContext context = getTaskAttemptContext(true, true, true);
        context.getConfiguration().set(PropertyConstants.COLUMN_SUBCOMPARATOR.txt,
                                       "invalidSubComparator");
        ssTableColumnRecordReader.initialize(inputSplit, context);
    }

    /**
     * Tests to make sure initialization doesn't fail when a sparse CF is specified
     */
    @Test
    public void testInitialize() throws Exception {
        Path inputPath = inputSplit.getPath();
        FileSystem remoteFS = FileSystem.get(inputPath.toUri(), conf);
        FileSystem localFS = FileSystem.getLocal(conf);
        TaskAttemptContext context = getTaskAttemptContext(true, true, true);
        SSTableInputFormat.setIsSparse(false, job);
        ssTableColumnRecordReader.initialize(inputSplit, context);
        verify(ssTableColumnRecordReader).copyTablesToLocal(remoteFS, localFS, inputPath, context);
        ssTableColumnRecordReader.close();
    }

    /**
     * Tests to see if tables can be correctly copied locally
     */
    @Test
    public void testCopyTablesToLocal() throws Exception {
        TaskAttemptContext context = getTaskAttemptContext(true, true, true);
        ssTableColumnRecordReader.initialize(inputSplit, context);

        doCallRealMethod().when(ssTableColumnRecordReader)
                          .copyTablesToLocal(any(FileSystem.class),
                                             any(FileSystem.class),
                                             any(Path.class),
                                             any(TaskAttemptContext.class));

        FileSystem remoteFS = mock(FileSystem.class);
        FileSystem localFS = mock(FileSystem.class);

        byte[] data = new byte[] { 0xA };
        FSDataInputStream fsIn = new FSDataInputStream(new MemoryDataInputStream(data));
        FSDataOutputStream fsOut = mock(FSDataOutputStream.class);

        when(remoteFS.open(any(Path.class))).thenReturn(fsIn);
        when(localFS.create(any(Path.class), anyBoolean())).thenReturn(fsOut);

        Path dataTablePath = inputSplit.getPath();
        FileStatus fileStatus = mock(FileStatus.class);
        when(fileStatus.getLen()).thenReturn(10L);
        when(fileStatus.isDirectory()).thenReturn(false);
        when(remoteFS.getFileStatus(any(Path.class))).thenReturn(fileStatus);

        ssTableColumnRecordReader.copyTablesToLocal(remoteFS, localFS, dataTablePath, context);
        verify(remoteFS).getFileStatus(dataTablePath);
        ssTableColumnRecordReader.close();
        verify(fsOut).write(any(byte[].class), eq(0), eq(data.length));
        assertEquals(2, ssTableColumnRecordReader.getComponentSize());
    }

    /**
     * Tests to see if tables can be correctly copied locally including the compression info table
     */
    @Test
    public void testCopyTablesToLocalWithCompressionInfo() throws Exception {
        TaskAttemptContext context = getTaskAttemptContext(true, true, true);
        ssTableColumnRecordReader.initialize(inputSplit, context);

        doCallRealMethod().when(ssTableColumnRecordReader)
                          .copyTablesToLocal(any(FileSystem.class),
                                             any(FileSystem.class),
                                             any(Path.class),
                                             any(TaskAttemptContext.class));

        FileSystem remoteFS = mock(FileSystem.class);
        FileSystem localFS = mock(FileSystem.class);

        byte[] data = new byte[] { 0xA };
        FSDataInputStream fsIn = new FSDataInputStream(new MemoryDataInputStream(data));
        FSDataOutputStream fsOut = mock(FSDataOutputStream.class);

        when(remoteFS.open(any(Path.class))).thenReturn(fsIn);
        when(localFS.create(any(Path.class), anyBoolean())).thenReturn(fsOut);

        Path dataTablePath = inputSplit.getPath();
        FileStatus fileStatus = mock(FileStatus.class);
        when(fileStatus.getLen()).thenReturn(10L);
        when(fileStatus.isDirectory()).thenReturn(false);
        when(remoteFS.getFileStatus(any(Path.class))).thenReturn(fileStatus);

        String str = ssTableColumnRecordReader.getDescriptor()
                                              .filenameFor(Component.COMPRESSION_INFO);
        when(remoteFS.exists(new Path(str))).thenReturn(true);

        ssTableColumnRecordReader.copyTablesToLocal(remoteFS, localFS, dataTablePath, context);
        verify(remoteFS).getFileStatus(dataTablePath);
        ssTableColumnRecordReader.close();
        verify(fsOut).write(any(byte[].class), eq(0), eq(data.length));
        assertEquals(3, ssTableColumnRecordReader.getComponentSize());
    }

    /**
     * Makes sure an exception is thrown when the data table path is a directory
     */
    @Test (expected = IllegalArgumentException.class)
    public void testCopyToLocalFileWithDirectoryPath() throws Exception {
        FileSystem remoteFS = mock(FileSystem.class);
        FileSystem localFS = mock(FileSystem.class);
        Path dataTablePath = inputSplit.getPath();
        Path localPath = new Path("local/path");
        FileStatus fileStatus = mock(FileStatus.class);
        when(fileStatus.getLen()).thenReturn(10L);
        when(fileStatus.isDirectory()).thenReturn(true);
        when(remoteFS.getFileStatus(dataTablePath)).thenReturn(fileStatus);

        ssTableColumnRecordReader.copyToLocalFile(remoteFS, localFS, dataTablePath, localPath);
    }
}
