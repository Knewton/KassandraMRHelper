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
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Helper class for testing the {@link SSTableRowMapper}. This implementation does nothing and
 * simply return the keys and values as is.
 *
 * @author Giannis Neokleous
 *
 */
public class DoNothingRowMapper extends
        SSTableRowMapper<ByteBuffer, SSTableIdentityIterator, Text, Text> {

    private ByteBuffer currentKey;
    private SSTableIdentityIterator currentRow;

    @Override
    public void performMapTask(ByteBuffer key, SSTableIdentityIterator row, Context context)
            throws IOException, InterruptedException {
        this.setCurrentKey(key);
        this.setCurrentRow(row);
    }

    @Override
    protected ByteBuffer getMapperKey(ByteBuffer key, Context context) {
        return key;
    }

    @Override
    protected SSTableIdentityIterator getMapperValue(SSTableIdentityIterator rowIter,
            Context context) {
        return rowIter;
    }

    public ByteBuffer getCurrentKey() {
        return currentKey;
    }

    public void setCurrentKey(ByteBuffer currentKey) {
        this.currentKey = currentKey;
    }

    public SSTableIdentityIterator getCurrentRow() {
        return currentRow;
    }

    public void setCurrentRow(SSTableIdentityIterator currentRow) {
        this.currentRow = currentRow;
    }

}
