package com.knewton.mapreduce;

import org.apache.cassandra.io.sstable.SSTableIdentityIterator;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Copyright (c) 2013 Knewton
 * 
 * Dual licensed under: MIT: http://www.opensource.org/licenses/mit-license.php GPLv3:
 * http://www.opensource.org/licenses/gpl-3.0.html.
 * 
 * Abstract mapper class that takes in a row key as its key and a column as its value.
 * 
 */
public class SSTableRowRecordReader extends SSTableRecordReader<ByteBuffer,
        SSTableIdentityIterator> {

    /**
     * Gets the next key and value in the table.
     * 
     * @return True if there's more keys. False otherwise.
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (tableScanner.hasNext()) {
            SSTableIdentityIterator row =
                    (SSTableIdentityIterator) tableScanner.next();
            currentKey = row.getKey().key;
            currentValue = row;
            incKeysRead(1);
            return true;
        }
        currentKey = null;
        currentValue = null;
        return false;
    }

}
