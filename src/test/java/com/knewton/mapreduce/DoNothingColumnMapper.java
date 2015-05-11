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

import org.apache.cassandra.db.IColumn;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.nio.ByteBuffer;

public class DoNothingColumnMapper extends SSTableColumnMapper<ByteBuffer, IColumn, Text, Text> {

    private ByteBuffer currentKey;
    private IColumn currentColumn;

    /**
     * {@inheritDoc}
     */
    @Override
    public void performMapTask(ByteBuffer key, IColumn column, Context context) throws IOException,
            InterruptedException {
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
