/**
 * Copyright (c) 2013 Knewton
 * 
 * Dual licensed under: MIT: http://www.opensource.org/licenses/mit-license.php GPLv3:
 * http://www.opensource.org/licenses/gpl-3.0.html.
 * 
 */
package com.knewton.mapreduce;

import com.google.common.collect.Sets;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.sstable.SSTableScanner;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyInputStream;

import java.io.*;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Abstract record reader class that handles keys and values from an sstable. It's subclassed by a
 * row record reader ({@link SSTableRowRecordReader}), passing an entire row as a key/value pair and
 * a column record reader ({@link SSTableColumnRecordReader}) passing individual columns as values.
 * Used in conjunction with {@linkSSTableInputFormat}
 * 
 * @param <K>
 * @param <V>
 */
public abstract class SSTableRecordReader<K, V> extends RecordReader<K, V> {

    public static final String COLUMN_COMPARATOR_PARAMETER =
            "com.knewton.cassandra.column.comparator";
    public static final String COLUMN_SUBCOMPARATOR_PARAMETER =
            "com.knewton.cassandra.column.subcomparator";
    public static final String COLUMN_FAMILY_TYPE_PARAMETER =
            "com.knewton.cassandra.cftype";
    public static final String PARTITIONER_PARAMETER =
            "com.knewton.partitioner";
    private static final String COMPRESSION_ENABLED_PARAMETER_NAME =
            "com.knewton.cassandra.backup.compression";
    private static final String DECOMPRESS_BUFFER_PARAMETER_NAME =
            "com.knewton.cassandra.backup.compress.buffersize";
    /**
     * Size of the decompression buffer in KBs.
     */
    private static final int DEFAULT_DECOMPRESS_BUFFER_SIZE = 512;

    protected SSTableScanner tableScanner;
    protected K currentKey;
    protected V currentValue;
    private SSTableReader tableReader;
    private long keysRead;
    private Set<Component> components;
    private Descriptor desc;

    private static final int REPORT_DECOMPRESS_PROGRESS_EVERY_GBS =
            1024 * 1024 * 1024; // 1GB
    private static final Logger LOG =
            LoggerFactory.getLogger(SSTableRecordReader.class);

    /**
     * Close all opened resources and delete temporary local files used for reading the data.
     * 
     * @throws IOException
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
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public K getCurrentKey() throws IOException, InterruptedException {
        return currentKey;
    }

    /**
     * Returns an iterator of the columns under the <code>currentKey<code>.
     * 
     * @return SSTableIdentityIterator Column iterator.
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public V getCurrentValue()
            throws IOException, InterruptedException {
        return currentValue;
    }

    /**
     * Method for calculating the progress made so far from this record reader.
     * 
     * @return A value from 0 to 1 indicating the progress so far.
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public float getProgress() throws IOException, InterruptedException {
        return Math.min((float) this.keysRead /
                (float) tableReader.estimatedKeys(), 1.0f);
    }

    /**
     * Performs all the necessary actions to initialize and prepare this record reader.
     * 
     * @param inputSplit
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context)
            throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        keysRead = 0;
        components = Sets.newHashSet();
        FileSplit split = (FileSplit) inputSplit;
        validateConfiguration(conf);
        // Get comparator. Subcomparator can be null.
        AbstractType<?> comparator = getConfComparator(
                conf, COLUMN_COMPARATOR_PARAMETER, "comparator");
        AbstractType<?> subcomparator = null;
        if (conf.get(COLUMN_SUBCOMPARATOR_PARAMETER) != null) {
            subcomparator = getConfComparator(
                    conf, COLUMN_SUBCOMPARATOR_PARAMETER, "subcomparator");
        }
        // Get partitioner for keys
        IPartitioner<?> partitioner = getConfPartitioner(
                conf, PARTITIONER_PARAMETER, "partitioner");
        // Column family type. Use Standard if property is not set.
        ColumnFamilyType columnFamilyType = getColumnFamilyType(conf);
        // Move minimum required db tables to local disk.
        copyTablesToLocal(split, context);
        // Open table and get scanner
        CFMetaData metadata = new CFMetaData(
                getDescriptor().ksname,
                getDescriptor().cfname,
                columnFamilyType,
                comparator,
                subcomparator);
        this.tableReader = SSTableReader.open(
                desc, components, metadata, partitioner);
        setTableScanner();
    }

    /**
     * Mainly here for testing.
     */
    private void setTableScanner() {
        if (tableReader != null) {
            this.tableScanner = tableReader.getDirectScanner();
        } else {
            throw new NullPointerException("Table reader not set.");
        }
    }

    /**
     * Mainly here for unit tests.
     * 
     * @return
     */
    public Descriptor getDescriptor() {
        return desc;
    }

    /**
     * Moves all the minimum required tables for the table reader to work to local disk.
     * 
     * @param split
     *            The table to work on.
     * @param context
     * @throws IOException
     */
    private void copyTablesToLocal(FileSplit split, TaskAttemptContext context)
            throws IOException {
        Path dataTablePath = split.getPath();
        Configuration conf = context.getConfiguration();
        FileSystem fs = FileSystem.get(dataTablePath.toUri(), conf);
        String hdfsDataTablePathStr = dataTablePath.toUri().getPath();
        String localDataTablePathStr = hdfsDataTablePathStr;
        // Make path relative due to EMR permissions
        if (localDataTablePathStr.startsWith("/")) {
            String mapTaskId = conf.get("mapred.task.id");
            String mapTempDir = conf.get("mapred.temp.dir");
            String taskWorkDir = mapTempDir + File.separator + mapTaskId;
            // String jobWorkDir = conf.get("job.local.dir");
            LOG.info("Appending {} to {}", taskWorkDir, localDataTablePathStr);
            localDataTablePathStr = taskWorkDir + localDataTablePathStr;
        }
        Path localDataTablePath = new Path(localDataTablePathStr);
        LOG.info("Copying hdfs file from {} to local disk at {}.",
                dataTablePath.toUri(), localDataTablePath.toUri());
        fs.copyToLocalFile(dataTablePath, localDataTablePath);
        boolean isCompressed =
                conf.getBoolean(COMPRESSION_ENABLED_PARAMETER_NAME, false);
        if (isCompressed) {
            decompress(localDataTablePath, context);
        }
        components.add(Component.DATA);
        desc = Descriptor.fromFilename(localDataTablePathStr);
        Descriptor hdfsDesc = Descriptor.fromFilename(hdfsDataTablePathStr);
        String indexPathStr = hdfsDesc.filenameFor(SSTable.COMPONENT_INDEX);
        components.add(Component.PRIMARY_INDEX);
        Path localIdxPath = new Path(desc.filenameFor(SSTable.COMPONENT_INDEX));
        LOG.info("Copying hdfs file from {} to local disk at {}.",
                indexPathStr, localIdxPath);
        fs.copyToLocalFile(new Path(indexPathStr), localIdxPath);
        if (isCompressed) {
            decompress(localIdxPath, context);
        }
        String compressionTablePathStr =
                hdfsDesc.filenameFor(Component.COMPRESSION_INFO.name());
        Path compressionTablePath = new Path(compressionTablePathStr);
        if (fs.exists(compressionTablePath)) {
            Path localCompressionPath =
                    new Path(desc.filenameFor(Component.COMPRESSION_INFO.name()));
            LOG.info("Copying hdfs file from {} to local disk at.",
                    compressionTablePath.toUri(),
                    localCompressionPath);
            fs.copyToLocalFile(compressionTablePath, localCompressionPath);
            if (isCompressed) {
                decompress(localCompressionPath, context);
            }
            components.add(Component.COMPRESSION_INFO);
        }
    }

    /**
     * Decompresses input files that were snappy compressed before opening them with the sstable
     * reader.It writes a new decompressed file with the same name as the compressed one. The old
     * one gets deleted.
     * 
     * @param localTablePath
     * @param context
     * @throws IOException
     */
    private void decompress(Path localTablePath, TaskAttemptContext context)
            throws IOException {
        context.setStatus(
                String.format("Decompressing %s", localTablePath.toUri()));
        int compressionBufSize = context.getConfiguration().getInt(
                DECOMPRESS_BUFFER_PARAMETER_NAME,
                DEFAULT_DECOMPRESS_BUFFER_SIZE);
        compressionBufSize *= 1024;
        LOG.info("Decompressing {} with buffer size {}.",
                localTablePath, compressionBufSize);
        File compressedFile = new File(localTablePath.toString());
        InputStream fis = new FileInputStream(compressedFile);
        InputStream bis = new BufferedInputStream(fis);
        InputStream sip = new SnappyInputStream(bis);
        File decompressedFile = new File(localTablePath.toString() + ".tmp");
        OutputStream os = new FileOutputStream(decompressedFile);
        OutputStream bos = new BufferedOutputStream(os);
        byte[] inByteArr = new byte[compressionBufSize];
        int bytesRead = 0;
        int bytesSinceLastReport = 0;
        while ((bytesRead = sip.read(inByteArr)) > 0) {
            bos.write(inByteArr, 0, bytesRead);
            bytesSinceLastReport += bytesRead;
            // Avoid timeouts. Report progress to the jobtracker.
            if (bytesSinceLastReport % REPORT_DECOMPRESS_PROGRESS_EVERY_GBS > 0) {
                context.setStatus(String.format("Decompressed %d bytes.",
                        bytesSinceLastReport));
                bytesSinceLastReport -= REPORT_DECOMPRESS_PROGRESS_EVERY_GBS;
            }
        }
        sip.close();
        bos.close();
        compressedFile.delete();
        decompressedFile.renameTo(compressedFile);
    }

    /**
     * Creates a column family type from <code>conf</code>
     * 
     * @param conf
     * @return A column family type. Simple or Super.
     */
    private ColumnFamilyType getColumnFamilyType(Configuration conf) {
        return ColumnFamilyType.create(conf.get(COLUMN_FAMILY_TYPE_PARAMETER));
    }

    /**
     * Get an instance of a partitioner.
     * 
     * @param conf
     * @param parameterName
     *            The name of the parameter to get from conf and instantiate.
     * @param instanceType
     *            Description of the instantiating parameter.
     * @return Instantiated object.
     */
    private <T> T getConfPartitioner(Configuration conf,
            String parameterName, String instanceType) {
        try {
            return FBUtilities.construct(
                    conf.get(parameterName), instanceType);
        } catch (ConfigurationException ce) {
            throw new IllegalArgumentException("Can't construct " + instanceType
                    + " from " + conf.get(parameterName) +
                    ". Got: " + ce.getMessage());
        }
    }

    /**
     * Get an instance of a comparator used for comparing keys in the sstables.
     * 
     * @param conf
     * @param parameterName
     *            The parameter name from the configuration object.
     * @param instanceType
     *            Description of the object being instantiated.
     * @return A new instance of the comparator.
     */
    private AbstractType<?> getConfComparator(Configuration conf,
            String parameterName, String instanceType) {
        try {
            return TypeParser.parse(conf.get(parameterName));
        } catch (ConfigurationException ce) {
            throw new IllegalArgumentException("Can't construct " + instanceType
                    + " from " + conf.get(parameterName) +
                    ". Got: " + ce.getMessage());
        }
    }

    /**
     * Minimum required parameters needed to be set for this type of record reader. Many other
     * parameters are inferred from the table filenames. Fail fast if conf parameters are missing.
     * 
     * @param conf
     */
    private void validateConfiguration(Configuration conf) {
        checkNotNull(conf.get(COLUMN_COMPARATOR_PARAMETER),
                COLUMN_COMPARATOR_PARAMETER + " not set.");
        checkNotNull(conf.get(PARTITIONER_PARAMETER),
                PARTITIONER_PARAMETER + " not set.");
    }

    /**
     * Increments the number of keys read from the data table.
     * 
     * @param val
     *            The value to be added to <code>keysRead<code>.
     */
    protected void incKeysRead(int val) {
        keysRead += val;
    }
}
