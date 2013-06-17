package com.knewton.mapreduce.io;

import com.knewton.mapreduce.SSTableColumnRecordReader;
import com.knewton.mapreduce.SSTableRecordReader;

import org.apache.cassandra.db.IColumn;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Copyright (c) 2013 Knewton
 * 
 * Dual licensed under: MIT: http://www.opensource.org/licenses/mit-license.php GPLv3:
 * http://www.opensource.org/licenses/gpl-3.0.html.
 * 
 * Input format that instantiates an sstable column record reader.
 * 
 */
public class SSTableColumnInputFormat extends SSTableInputFormat<ByteBuffer,
        IColumn> {

    /**
     * Create a new record reader for this <code>split<code>.
     * 
     * @param split
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public SSTableRecordReader<ByteBuffer, IColumn> createRecordReader(
            InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        return new SSTableColumnRecordReader();
    }

}
