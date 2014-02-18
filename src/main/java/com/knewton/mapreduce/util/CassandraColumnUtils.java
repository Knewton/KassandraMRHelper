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
package com.knewton.mapreduce.util;

import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.SuperColumn;
import org.apache.cassandra.io.sstable.SSTableIdentityIterator;

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
