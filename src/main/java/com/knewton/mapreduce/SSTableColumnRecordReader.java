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

import org.apache.cassandra.db.OnDiskAtom;
import org.apache.cassandra.io.sstable.SSTableIdentityIterator;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Record reader for sstables that calls map with the key being the sstable row key and the value a
 * single column.
 * 
 */
public class SSTableColumnRecordReader extends
        SSTableRecordReader<ByteBuffer, OnDiskAtom> {

    private SSTableIdentityIterator row;

    /**
     * Gets the next key and value in the table.
     * 
     * @return True if there's more keys. False otherwise.
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        // Read until there's a row with columns
        while ((row == null || !row.hasNext()) && tableScanner.hasNext()) {
            row = (SSTableIdentityIterator) tableScanner.next();
            currentKey = row.getKey().key;
            incKeysRead(1);
        }
        if (row != null && row.hasNext()) {
            currentValue = row.next();
            return true;
        }
        currentKey = null;
        currentValue = null;
        row = null;
        return false;
    }

}
