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

import org.apache.cassandra.db.DeletedColumn;
import org.apache.cassandra.db.IColumn;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Abstract mapper class that takes in a row key as its key and a column as its value. Used in
 * conjunction with {@link SSTableColumnRecordReader}
 * 
 * @param <K2>
 *            Out key
 * @param <V2>
 *            Out value
 */
@SuppressWarnings("rawtypes")
public abstract class SSTableColumnMapper<K1, V1, K2 extends WritableComparable, V2 extends Writable>
        extends Mapper<ByteBuffer, IColumn, K2, V2> {

    private boolean skipDeletedColumns;
    private ByteBuffer key;
    private IColumn iColumn;

    public SSTableColumnMapper() {
        skipDeletedColumns = false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void map(ByteBuffer key, IColumn iColumn,
            Context context) throws IOException, InterruptedException {
        if (skipDeletedColumns && iColumn instanceof DeletedColumn) {
            return;
        }
        K1 mapKey = getMapperKey(key, context);
        if (mapKey == null) {
            return;
        }
        V1 mapValue = getMapperValue(iColumn, context);
        if (mapValue == null) {
            return;
        }
        this.iColumn = iColumn;
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
     * 
     * @return True if deleted columns will be skipped false otherwise.
     */
    public boolean isSkipDeletedColumns() {
        return skipDeletedColumns;
    }

    /**
     * 
     * @param skipDeletedColumns
     */
    public void setSkipDeletedColumns(boolean skipDeletedColumns) {
        this.skipDeletedColumns = skipDeletedColumns;
    }

    /**
     * Any modifications to the column name <code>ByteBuffer</code> or the column value
     * <code>ByteBuffer</code> needs to be rewinded. Make sure you know what you're doing if you
     * call this.
     * 
     * @return The current cassandra column.
     */
    protected IColumn getIColumn() {
        return iColumn;
    }

    /**
     * Any modifications to this row key bytebuffer should be rewinded. Make sure you know what
     * you're doing if you call this.
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
     * @return Mapper key of type <code>K1</code>
     */
    protected abstract K1 getMapperKey(ByteBuffer key, Context context);

    /**
     * Get the mapper specific value that would make an SSTable column into something more
     * meaningful.
     * 
     * @param iColumn
     * @param context
     * @return Mapper value of type <code>V1</code>
     */
    protected abstract V1 getMapperValue(IColumn iColumn, Context context);
}
