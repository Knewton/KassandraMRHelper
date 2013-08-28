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
package com.knewton.mapreduce.io;

import static org.junit.Assert.*;

import com.knewton.mapreduce.io.SSTableInputFormat.DataTablePathFilter;

import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class DataTablePathFilterTest {

    /**
     * Tests to see if the table path filter can correctly filter through the sstables and only get
     * the data tables.
     */
    @Test
    public void testDataTablePathFilter() {
        DataTablePathFilter pathFilter = new DataTablePathFilter();
        assertTrue(pathFilter.accept(new Path("/some/path/table-g-321-Data.db")));
        assertTrue(pathFilter.accept(new Path("path/table-g-321-Data.db")));
        assertFalse(pathFilter.accept(new Path("/some/path/table-g-321-DATA.db")));
        assertFalse(pathFilter.accept(new Path("/path/table-g-321-Index.db")));
        assertFalse(pathFilter.accept(new Path("path/table-g-321-INDEX.db")));
        assertFalse(pathFilter.accept(new Path("/some/path/table-g-321-Index.db")));
        assertFalse(pathFilter.accept(new Path("/")));
        assertFalse(pathFilter.accept(null));
        // temp file, don't accept.
        assertFalse(pathFilter.accept(new Path("/some/path/table-tmp-g-321-Index.db")));
    }

}
