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

import org.apache.cassandra.io.sstable.SSTableIdentityIterator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Abstract mapper class that takes in a row key as its key and a column as its value. Used in
 * conjunction with {@link SSTableRowRecordReader}
 *
 * @author Giannis Neokleous
 *
 * @param <K1>
 * @param <V1>
 * @param <K2>
 * @param <V2>
 */
public abstract class SSTableRowMapper<K1, V1, K2 extends WritableComparable<?>, V2 extends Writable>
        extends Mapper<ByteBuffer, SSTableIdentityIterator, K2, V2> {

    private ByteBuffer key;
    private SSTableIdentityIterator rowIter;

    /**
     * {@inheritDoc}
     */
    @Override
    public void map(ByteBuffer key, SSTableIdentityIterator rowIter, Context context)
            throws IOException, InterruptedException {

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
     */
    public abstract void performMapTask(K1 key, V1 value, Context context) throws IOException,
            InterruptedException;

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
     * @return The mapper key read from a {@link ByteBuffer}
     */
    protected abstract K1 getMapperKey(ByteBuffer key, Context context);

    /**
     * Get the mapper specific value that would make a row iterator into something more meaningful.
     *
     * @param rowIter
     * @param context
     * @return The mapper value read from a cassandra row
     */
    protected abstract V1 getMapperValue(SSTableIdentityIterator rowIter, Context context);

}
