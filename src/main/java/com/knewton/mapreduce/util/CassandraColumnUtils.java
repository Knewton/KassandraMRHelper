package com.knewton.mapreduce.util;

import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.SuperColumn;
import org.apache.cassandra.io.sstable.SSTableIdentityIterator;

/**
 * Copyright (c) 2013 Knewton
 * 
 * Dual licensed under: MIT: http://www.opensource.org/licenses/mit-license.php GPLv3:
 * http://www.opensource.org/licenses/gpl-3.0.html.
 * 
 */
public class CassandraColumnUtils {

    /**
     * Helper method for figuring out if a row has super columns.
     * 
     * @param row
     * @return True if row contains super columns false otherwise.
     */
    public static boolean isSuperColumn(SSTableIdentityIterator row) {
        return row.getColumnFamily().isSuper();
    }

    /**
     * Helper method for figuring out if a column is an instance of a <code>SuperColumn</code>
     * 
     * @param column
     * @return True if super column false otherwise.
     */
    public static boolean isSuperColumn(IColumn column) {
        if (column instanceof SuperColumn) {
            return true;
        }
        return false;
    }

}
