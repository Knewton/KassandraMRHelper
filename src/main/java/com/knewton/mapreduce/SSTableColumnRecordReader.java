package com.knewton.mapreduce;

import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.io.sstable.SSTableIdentityIterator;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Copyright (c) 2013 Knewton
 * 
 * Dual licensed under: MIT: http://www.opensource.org/licenses/mit-license.php GPLv3:
 * http://www.opensource.org/licenses/gpl-3.0.html.
 * 
 * Record reader for sstables that calls map with the key being the sstable row key and the value a
 * single column.
 * 
 */
public class SSTableColumnRecordReader extends
        SSTableRecordReader<ByteBuffer, IColumn> {

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
