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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyInputStream;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Set;

import javax.annotation.Nullable;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Abstract record reader class that handles keys and values from an sstable. It's subclassed by a
 * row record reader ({@link SSTableRowRecordReader}), passing an entire row as a key/value pair and
 * a disk atom record reader ({@link SSTableColumnRecordReader}) passing individual disk atoms as
 * values. Used in conjunction with {@link SSTableInputFormat}
 *
 * @author Giannis Neokleous
 *
 * @param <K>
 *            Key in type
 * @param <V>
 *            Value in type
 */
public abstract class SSTableRecordReader<K, V> extends RecordReader<K, V> {

    /**
     * Size of the decompression buffer in KBs.
     */
    private static final int DEFAULT_DECOMPRESS_BUFFER_SIZE = 512;
    private static final int REPORT_DECOMPRESS_PROGRESS_EVERY_GBS = 1024 * 1024 * 1024; // 1GB
    private static final Logger LOG = LoggerFactory.getLogger(SSTableRecordReader.class);

    protected ISSTableScanner tableScanner;
    protected K currentKey;
    protected V currentValue;
    private long keysRead;
    private Set<Component> components;
    private Descriptor desc;
    private long estimatedKeys;
    private long minTimestampMs = Long.MIN_VALUE;
    private long maxTimestampMs = Long.MAX_VALUE;
    private Configuration conf;
    private TaskAttemptContext ctx;

    /**
     * Close all opened resources and delete temporary local files used for reading the data.
     */
    @Override
    public void close() throws IOException {
        tableScanner.close();
        // cleanup files in localdisk
        File componentFile;
        for (Component component : components) {
            componentFile = new File(desc.filenameFor(component));
            if (componentFile.exists()) {
                LOG.info("Deleting {}", componentFile.toURI());
                componentFile.delete();
            }
        }
    }

    /**
     * Returns the value of the current key.
     *
     * @return The current key in the data table.
     */
    @Override
    public K getCurrentKey() throws IOException, InterruptedException {
        return currentKey;
    }

    /**
     * Returns an iterator of the columns under the <code>currentKey<code>.
     *
     * @return SSTableIdentityIterator Column iterator.
     */
    @Override
    public V getCurrentValue() throws IOException, InterruptedException {
        return currentValue;
    }

    /**
     * Method for calculating the progress made so far from this record reader.
     *
     * @return A value from 0 to 1 indicating the progress so far.
     */
    @Override
    public float getProgress() throws IOException, InterruptedException {
        return Math.min((float) this.keysRead / (float) estimatedKeys, 1.0f);
    }

    /**
     * Performs all the necessary actions to initialize and prepare this record reader.
     */
    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context)
            throws IOException, InterruptedException {
        this.ctx = context;
        conf = context.getConfiguration();
        keysRead = 0;
        components = Sets.newHashSetWithExpectedSize(3);
        FileSplit split = (FileSplit) inputSplit;
        validateConfiguration(conf);

        // Get comparator. Subcomparator can be null.
        AbstractType<?> comparator = getConfComparator(conf);
        AbstractType<?> subcomparator = getConfComparator(conf);

        // Get partitioner for keys
        IPartitioner partitioner = getConfPartitioner(conf);

        // Move minimum required db tables to local disk.
        copyTablesToLocal(split, context);
        CFMetaData cfMetaData;
        if (getConfIsSparse(conf)) {
            cfMetaData = CFMetaData.sparseCFMetaData(getDescriptor().ksname, getDescriptor().cfname,
                                                     comparator);
        } else {
            cfMetaData = CFMetaData.denseCFMetaData(getDescriptor().ksname, getDescriptor().cfname,
                                                    comparator, subcomparator);
        }
        // Open table and get scanner
        SSTableReader tableReader = openSSTableReader(partitioner, cfMetaData);
        setTableScanner(tableReader);
    }

    @VisibleForTesting
    SSTableReader openSSTableReader(IPartitioner partitioner, CFMetaData metadata)
            throws IOException {
        LOG.info("Open SSTable {}", desc);
        return SSTableReader.openForBatch(desc, components, metadata, partitioner);
    }

    private void setTableScanner(SSTableReader tableReader) {
        Preconditions.checkNotNull(tableReader, "Table reader not set");
        this.tableScanner = tableReader.getScanner();
        this.estimatedKeys = tableReader.estimatedKeys();
    }

    /**
     * Mainly here for unit tests.
     *
     * @return SSTable descriptor.
     */
    @VisibleForTesting
    protected Descriptor getDescriptor() {
        return desc;
    }

    /**
     * Keep track of the min and max timestamps in milliseconds NOTE: Cassandra timestamps are
     * usually given in microseconds Presume that the input is in microseconds and divide to get
     * milliseconds
     */
    protected void updateTimeInterval(long timestamp) {
        long checkTimestamp = timestamp / 1000L;
        if ((minTimestampMs > checkTimestamp) || (minTimestampMs == Long.MIN_VALUE)) {
            minTimestampMs = checkTimestamp;
        }
        if ((maxTimestampMs < checkTimestamp) || (maxTimestampMs == Long.MAX_VALUE)) {
            maxTimestampMs = checkTimestamp;
        }
    }

    /**
     * Moves all the minimum required tables for the table reader to work to local disk.
     *
     * @param split The table to work on.
     */
    @VisibleForTesting
    void copyTablesToLocal(FileSplit split, TaskAttemptContext context) throws IOException {
        Path dataTablePath = split.getPath();
        Configuration conf = context.getConfiguration();
        FileSystem fs = FileSystem.get(dataTablePath.toUri(), conf);
        String hdfsDataTablePathStr = dataTablePath.toUri().getPath();
        String localDataTablePathStr = dataTablePath.toUri().getHost() +
                                      File.separator + dataTablePath.toUri().getPath();
        // Make path relative due to EMR permissions
        if (localDataTablePathStr.startsWith("/")) {
            String mapTaskId = conf.get("mapreduce.task.attempt.id");
            String mapTempDir = conf.get("mapreduce.cluster.temp.dir");
            String taskWorkDir = mapTempDir + File.separator + mapTaskId;
            // String jobWorkDir = conf.get("job.local.dir");
            LOG.info("Appending {} to {}", taskWorkDir, localDataTablePathStr);
            localDataTablePathStr = taskWorkDir + localDataTablePathStr;
        }
        Path localDataTablePath = new Path(localDataTablePathStr);
        LOG.info("Copying hdfs file from {} to local disk at {}.", dataTablePath.toUri(),
                 localDataTablePath.toUri());
        copyToLocalFile(fs, dataTablePath, localDataTablePath);
        boolean isCompressed = conf.getBoolean(PropertyConstants.COMPRESSION_ENABLED.txt, false);
        if (isCompressed) {
            decompress(localDataTablePath, context);
        }
        components.add(Component.DATA);
        desc = Descriptor.fromFilename(localDataTablePathStr);
        Descriptor hdfsDesc = Descriptor.fromFilename(hdfsDataTablePathStr);
        String indexPathStr = hdfsDesc.filenameFor(Component.PRIMARY_INDEX);
        components.add(Component.PRIMARY_INDEX);
        Path localIdxPath = new Path(desc.filenameFor(Component.PRIMARY_INDEX));
        LOG.info("Copying hdfs file from {} to local disk at {}.", indexPathStr, localIdxPath);
        copyToLocalFile(fs, new Path(indexPathStr), localIdxPath);
        if (isCompressed) {
            decompress(localIdxPath, context);
        }
        String compressionTablePathStr = hdfsDesc.filenameFor(Component.COMPRESSION_INFO.name());
        Path compressionTablePath = new Path(compressionTablePathStr);
        if (fs.exists(compressionTablePath)) {
            Path localCompressionPath = new Path(
                    desc.filenameFor(Component.COMPRESSION_INFO.name()));
            LOG.info("Copying hdfs file from {} to local disk at {}.", compressionTablePath.toUri(),
                     localCompressionPath);
            copyToLocalFile(fs, compressionTablePath, localCompressionPath);
            if (isCompressed) {
                decompress(localCompressionPath, context);
            }
            components.add(Component.COMPRESSION_INFO);
        }
    }

    /**
     * Copies a remote path to the local filesystem, while updating hadoop that we're making
     * progress. Doesn't support directories.
     */
    private void copyToLocalFile(FileSystem remoteFS, Path remote, Path local) throws IOException {
        FileSystem localFS = FileSystem.getLocal(this.conf);
        // don't support transferring from remote directories
        FileStatus remoteStat = remoteFS.getFileStatus(remote);
        if (remoteStat.isDirectory()) {
            throw new RuntimeException("Path " + remote + " is directory!");
        }
        // if local is a dir, copy to inside that dir, like 'cp /path/file /tmp/' would do
        if (localFS.exists(local)) {
            FileStatus localStat = localFS.getFileStatus(local);
            if (localStat.isDirectory()) {
                local = new Path(local, remote.getName());
            }
        }
        long remoteFileSize = remoteStat.getLen();
        // do actual copy
        InputStream in = null;
        OutputStream out = null;
        try {
            long startTime = System.currentTimeMillis();
            long lastLogTime = 0;
            long bytesCopied = 0;
            in = remoteFS.open(remote);
            out = localFS.create(local, true);
            int buffSize = this.conf.getInt(CommonConfigurationKeys.IO_FILE_BUFFER_SIZE_KEY,
                                            CommonConfigurationKeys.IO_FILE_BUFFER_SIZE_DEFAULT);
            byte buf[] = new byte[buffSize];
            int bytesRead = in.read(buf);
            while (bytesRead >= 0) {
                long now = System.currentTimeMillis();
                // log transfer rate once per min, starting 1 min after transfer began
                if (now - lastLogTime > 60000L && now - startTime > 60000L) {
                    double elapsedSec = (now - startTime) / 1000D;
                    double bytesPerSec = bytesCopied / elapsedSec;
                    LOG.info("Transferred {} of {} bytes at {} bytes per second", bytesCopied,
                             remoteFileSize, bytesPerSec);
                    lastLogTime = now;
                }
                this.ctx.progress();
                out.write(buf, 0, bytesRead);
                bytesCopied += bytesRead;
                bytesRead = in.read(buf);
            }
            // try to close these outside of finally so we receive exception on failure
            out.close();
            out = null;
            in.close();
            in = null;
        } finally {
            // make sure everything's closed
            IOUtils.closeStream(out);
            IOUtils.closeStream(in);
        }
    }

    /**
     * Decompresses input files that were snappy compressed before opening them with the sstable
     * reader. It writes a new decompressed file with the same name as the compressed one. The old
     * one gets deleted.
     */
    private void decompress(Path localTablePath, TaskAttemptContext context) throws IOException {
        context.setStatus(String.format("Decompressing %s", localTablePath.toUri()));
        int compressionBufSize = context.getConfiguration().getInt(
                PropertyConstants.DECOMPRESS_BUFFER.txt, DEFAULT_DECOMPRESS_BUFFER_SIZE);
        compressionBufSize *= 1024;
        LOG.info("Decompressing {} with buffer size {}.", localTablePath, compressionBufSize);
        File compressedFile = new File(localTablePath.toString());
        InputStream fis = new FileInputStream(compressedFile);
        InputStream bis = new BufferedInputStream(fis, compressionBufSize);
        InputStream sip = new SnappyInputStream(bis);
        File decompressedFile = new File(localTablePath.toString() + ".tmp");

        OutputStream os = new FileOutputStream(decompressedFile);
        OutputStream bos = new BufferedOutputStream(os, compressionBufSize);
        byte[] inByteArr = new byte[compressionBufSize];
        int bytesRead = 0;
        int bytesSinceLastReport = 0;
        while ((bytesRead = sip.read(inByteArr)) > 0) {
            bos.write(inByteArr, 0, bytesRead);
            bytesSinceLastReport += bytesRead;
            // Avoid timeouts. Report progress to the jobtracker.
            if (bytesSinceLastReport % REPORT_DECOMPRESS_PROGRESS_EVERY_GBS > 0) {
                context.setStatus(String.format("Decompressed %d bytes.", bytesSinceLastReport));
                bytesSinceLastReport -= REPORT_DECOMPRESS_PROGRESS_EVERY_GBS;
            }
        }
        sip.close();
        bos.close();
        compressedFile.delete();
        decompressedFile.renameTo(compressedFile);
    }

    /**
     * @return True if the columns are sparse, false if they're dense
     */
    private boolean getConfIsSparse(Configuration conf) {
        return conf.getBoolean(PropertyConstants.SPARSE_COLUMN.txt, true);
    }

    /**
     * Get an instance of a partitioner.
     *
     * @param conf The configuration object
     * @return Instantiated partitioner object.
     */
    private <T> T getConfPartitioner(Configuration conf) {
        String partitionerStr = conf.get(PropertyConstants.PARTITIONER.txt);

        try {
            return FBUtilities.construct(partitionerStr, "partitioner");
        } catch (ConfigurationException ce) {
            String msg = String.format("Can't construct partitioner from %s", partitionerStr);
            throw new IllegalArgumentException(msg, ce);
        }
    }

    /**
     * Get an instance of a comparator used for comparing keys in the sstables.
     *
     * @param conf The configuration object
     * @return A new instance of the comparator.
     */
    private AbstractType<?> getConfComparator(Configuration conf) {
        String comparatorStr = conf.get(PropertyConstants.COLUMN_COMPARATOR.txt);
        Preconditions.checkNotNull(comparatorStr,
                                   String.format("Property %s not set",
                                                 PropertyConstants.COLUMN_COMPARATOR.txt));
        try {
            return TypeParser.parse(comparatorStr);
        } catch (SyntaxException | ConfigurationException ce) {
            String msg = String.format("Can't construct comparator from %s.", comparatorStr);
            throw new IllegalArgumentException(msg, ce);
        }
    }

    /**
     * Get an instance of a subcomparator used for comparing keys in the sstables.
     *
     * @param conf The configuration object
     * @return A new instance of the subcomparator.
     */
    @Nullable
    private AbstractType<?> getConfSubComparator(Configuration conf) {
        String subcomparatorStr = conf.get(PropertyConstants.COLUMN_SUBCOMPARATOR.txt);
        if (subcomparatorStr == null) {
            return null;
        }

        try {
            return TypeParser.parse(subcomparatorStr);
        } catch (SyntaxException | ConfigurationException ce) {
            String msg = String.format("Can't construct subcomparator from %s.", subcomparatorStr);
            throw new IllegalArgumentException(msg, ce);
        }
    }

    /**
     * Minimum required parameters needed to be set for this type of record reader. Many other
     * parameters are inferred from the table filenames. Fail fast if conf parameters are missing.
     */
    private void validateConfiguration(Configuration conf) {
        checkNotNull(conf.get(PropertyConstants.COLUMN_COMPARATOR.txt),
                     PropertyConstants.COLUMN_COMPARATOR.txt + " not set.");
        checkNotNull(conf.get(PropertyConstants.PARTITIONER.txt),
                     PropertyConstants.PARTITIONER.txt + " not set.");
    }

    /**
     * Increments the number of keys read from the data table.
     *
     * @param val The value to be added to <code>keysRead<code>.
     */
    protected void incKeysRead(int val) {
        keysRead += val;
    }
}
