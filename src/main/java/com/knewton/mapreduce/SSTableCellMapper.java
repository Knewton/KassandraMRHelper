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

import org.apache.cassandra.db.Cell;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Abstract mapper class that takes in a row key as its key and an {@link Cell} as its value. Used
 * in conjunction with {@link SSTableColumnRecordReader}. You should inherit from this mapper if you
 * want to read one column at a time. See also, {@link SSTableOnDiskAtomMapper} which will allow you
 * to also read {@link RangeTombstone}s
 *
 * @author Giannis Neokleous
 *
 * @param <K1>
 *            In key
 * @param <V1>
 *            In value
 * @param <K2>
 *            Out key
 * @param <V2>
 *            Out value
 */
public abstract class SSTableCellMapper<K1, V1,
                                        K2 extends WritableComparable<?>, V2 extends Writable>
        extends AbstractSSTableMapper<Cell, K1, V1, K2, V2> {

    public SSTableCellMapper() {
        super(Cell.class);
    }

}
