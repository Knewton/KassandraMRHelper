/**
 * Copyright (c) 2013 Knewton
 * 
 * Dual licensed under: MIT: http://www.opensource.org/licenses/mit-license.php GPLv3:
 * http://www.opensource.org/licenses/gpl-3.0.html.
 * 
 */
package com.knewton.mapreduce;

import org.apache.cassandra.io.sstable.SSTableIdentityIterator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Abstract mapper class that takes in a row key as its key and a column as its value. Used in
 * conjunction with {@linkSSTableRowRecordReader}
 * 
 * @author Giannis Neokleous
 * 
 * @param <K1>
 * @param <V1>
 * @param <K2>
 * @param <V2>
 */
@SuppressWarnings("rawtypes")
public abstract class SSTableRowMapper<K1, V1, K2 extends WritableComparable, V2 extends Writable>
        extends Mapper<ByteBuffer, SSTableIdentityIterator, K2, V2> {

    private ByteBuffer key;
    private SSTableIdentityIterator rowIter;

    /**
     * {@inheritDoc}
     */
    @Override
    public void map(ByteBuffer key, SSTableIdentityIterator rowIter,
            Context context) throws IOException, InterruptedException {
        K1 mapKey = getMapperKey(key, context);
        if (mapKey == null) {
            return;
        }
        V1 mapValue = getMapperValue(rowIter, context);
        if (mapValue == null) {
            return;
        }
        this.rowIter = rowIter;
        this.key = key;
        performMapTask(mapKey, mapValue, context);
    }

    /**
     * This should be defined in the child class to output any key/value pairs that need to go to a
     * reducer.
     * 
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    public abstract void performMapTask(K1 key, V1 value,
            Context context) throws IOException, InterruptedException;

    /**
     * Any modifications to the row key <code>ByteBuffer</code> or the row iterator
     * <code>SSTableIdentityIterator</code> needs to be rewinded. Make sure you know what you're
     * doing if you call this.
     * 
     * @return The current Cassandra row iterator.
     */
    protected SSTableIdentityIterator getRowIterator() {
        return rowIter;
    }

    /**
     * Any modifications to this row key <code>ByteBuffer</code> should be rewinded. Make sure you
     * know what you're doing if you call this.
     * 
     * @return The original <code>ByteBuffer</code> representing the row key.
     */
    protected ByteBuffer getRowKey() {
        return key;
    }

    /**
     * Get the mapper specific key that would make the sstable row key into something more
     * meaningful.
     * 
     * @param key
     * @param context
     * @return
     */
    protected abstract K1 getMapperKey(ByteBuffer key, Context context);

    /**
     * Get the mapper specific value that would make a row iterator into something more meaningful.
     * 
     * @param rowIter
     * @param context
     * @return
     */
    protected abstract V1 getMapperValue(SSTableIdentityIterator rowIter,
            Context context);

}
