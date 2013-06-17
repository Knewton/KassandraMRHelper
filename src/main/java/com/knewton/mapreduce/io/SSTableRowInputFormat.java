package com.knewton.mapreduce.io;

import com.knewton.mapreduce.SSTableRecordReader;
import com.knewton.mapreduce.SSTableRowRecordReader;

import org.apache.cassandra.io.sstable.SSTableIdentityIterator;
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
 * Input format that instantiates an sstable row record reader.
 * 
 */
public class SSTableRowInputFormat extends SSTableInputFormat<ByteBuffer,
        SSTableIdentityIterator> {

    /**
     * Create a new record reader for this <code>split<code>.
     * 
     * @param split
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public SSTableRecordReader<ByteBuffer, SSTableIdentityIterator>
            createRecordReader(InputSplit split, TaskAttemptContext context)
                    throws IOException, InterruptedException {
        return new SSTableRowRecordReader();
    }
}
