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

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Abstract mapper class that takes in a row key as its key and a column as its value.
 *
 * @author Giannis Neokleous
 *
 */
public class SSTableRowRecordReader extends SSTableRecordReader<ByteBuffer,
        SSTableIdentityIterator> {

    /**
     * Gets the next key and value in the table.
     *
     * @return True if there's more keys. False otherwise.
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (tableScanner.hasNext()) {
            SSTableIdentityIterator row = (SSTableIdentityIterator) tableScanner.next();
            currentKey = row.getKey().getKey();
            currentValue = row;
            incKeysRead(1);
            return true;
        }
        currentKey = null;
        currentValue = null;
        return false;
    }

}
