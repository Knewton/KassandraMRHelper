package com.knewton.mapreduce.io;

import static org.junit.Assert.*;

import com.knewton.mapreduce.io.SSTableInputFormat.DataTablePathFilter;

import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class DataTablePathFilterTest {

    /**
     * Tests to see if the table path filter can correctly filter through
     * the sstables and only get the data tables.
     */
    @Test
    public void testDataTablePathFilter() {
        DataTablePathFilter pathFilter = new DataTablePathFilter();
        assertTrue(pathFilter.accept(new Path("/some/path/table-Data.db")));
        assertTrue(pathFilter.accept(new Path("table-Data.db")));
        assertFalse(pathFilter.accept(new Path("/some/path/table-DATA.db")));
        assertFalse(pathFilter.accept(new Path("table-Index.db")));
        assertFalse(pathFilter.accept(new Path("table-INDEX.db")));
        assertFalse(pathFilter.accept(new Path("/some/path/table-Index.db")));
        assertFalse(pathFilter.accept(new Path("/")));
        assertFalse(pathFilter.accept(null));
    }

}
