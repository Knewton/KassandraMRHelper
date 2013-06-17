package com.knewton.mapreduce;

import org.apache.cassandra.db.IColumn;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.nio.ByteBuffer;

public class DoNothingColumnMapper extends SSTableColumnMapper<ByteBuffer,
        IColumn, Text, Text> {

    private ByteBuffer currentKey;
    private IColumn currentColumn;

    /**
     * {@inheritDoc}
     */
    @Override
    public void performMapTask(ByteBuffer key, IColumn column,
            Context context) throws IOException, InterruptedException {
        this.setCurrentKey(key);
        this.setCurrentColumn(column);

    }

    public IColumn getCurrentColumn() {
        return currentColumn;
    }

    public void setCurrentColumn(IColumn currentColumn) {
        this.currentColumn = currentColumn;
    }

    public ByteBuffer getCurrentKey() {
        return currentKey;
    }

    public void setCurrentKey(ByteBuffer currentKey) {
        this.currentKey = currentKey;
    }

    public void clearKeyVal() {
        this.currentColumn = null;
        this.currentKey = null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ByteBuffer getMapperKey(ByteBuffer key, Context context) {
        return key;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IColumn getMapperValue(IColumn value, Context context) {
        return value;
    }

}
