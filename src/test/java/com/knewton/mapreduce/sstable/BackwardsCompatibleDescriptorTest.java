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
package com.knewton.mapreduce.sstable;

import com.knewton.mapreduce.io.sstable.BackwardsCompatibleDescriptor;

import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class BackwardsCompatibleDescriptorTest {

    /**
     * Tests older version sstables with the keyspace name not part of the filename.
     */
    @Test
    public void testOldSSTableFromFilename() {
        Descriptor desc = BackwardsCompatibleDescriptor.fromFilename("ks/cf-g-123-Data.db");
        assertEquals("ks", desc.ksname);
        assertEquals("cf", desc.cfname);
        assertEquals(false, desc.temporary);
        assertEquals("g", desc.version.toString());
        assertEquals(123, desc.generation);

        desc = BackwardsCompatibleDescriptor.fromFilename("ks/cf-tmp-g-123-Index.db");
        assertEquals("ks", desc.ksname);
        assertEquals("cf", desc.cfname);
        assertEquals(true, desc.temporary);
        assertEquals("g", desc.version.toString());
        assertEquals(123, desc.generation);

        desc = BackwardsCompatibleDescriptor.fromFilename("ks/tmp-tmp-g-123-Index.db");
        assertEquals("ks", desc.ksname);
        assertEquals("tmp", desc.cfname);
        assertEquals(true, desc.temporary);
        assertEquals("g", desc.version.toString());
        assertEquals(123, desc.generation);

        desc = BackwardsCompatibleDescriptor.fromFilename("ks/tmp-g-123-Index.db");
        assertEquals("ks", desc.ksname);
        assertEquals("tmp", desc.cfname);
        assertEquals(false, desc.temporary);
        assertEquals("g", desc.version.toString());
        assertEquals(123, desc.generation);

        desc = BackwardsCompatibleDescriptor.fromFilename("tmp/tmp-g-123-Index.db");
        assertEquals("tmp", desc.ksname);
        assertEquals("tmp", desc.cfname);
        assertEquals(false, desc.temporary);
        assertEquals("g", desc.version.toString());
        assertEquals(123, desc.generation);

        desc = BackwardsCompatibleDescriptor.fromFilename("tmp/tmp-tmp-g-123-Index.db");
        assertEquals("tmp", desc.ksname);
        assertEquals("tmp", desc.cfname);
        assertEquals(true, desc.temporary);
        assertEquals("g", desc.version.toString());
        assertEquals(123, desc.generation);

        desc = BackwardsCompatibleDescriptor.fromFilename("tmp/cf-tmp-g-123-Index.db");
        assertEquals("tmp", desc.ksname);
        assertEquals("cf", desc.cfname);
        assertEquals(true, desc.temporary);
        assertEquals("g", desc.version.toString());
        assertEquals(123, desc.generation);
    }

    /**
     * Tests newer version sstables with the keyspace name as part of the filename.
     */
    @Test
    public void testSSTableFromFilename() {
        Descriptor desc = BackwardsCompatibleDescriptor.fromFilename("dir/ks-cf-ic-123-Data.db");
        assertEquals("ks", desc.ksname);
        assertEquals("cf", desc.cfname);
        assertEquals(false, desc.temporary);
        assertEquals("ic", desc.version.toString());
        assertEquals(123, desc.generation);

        desc = BackwardsCompatibleDescriptor.fromFilename("dir/ks-cf-tmp-ic-123-Data.db");
        assertEquals("ks", desc.ksname);
        assertEquals("cf", desc.cfname);
        assertEquals(true, desc.temporary);
        assertEquals("ic", desc.version.toString());
        assertEquals(123, desc.generation);

        desc = BackwardsCompatibleDescriptor.fromFilename("dir/ks-tmp-tmp-ic-123-Index.db");
        assertEquals("ks", desc.ksname);
        assertEquals("tmp", desc.cfname);
        assertEquals(true, desc.temporary);
        assertEquals("ic", desc.version.toString());
        assertEquals(123, desc.generation);

        desc = BackwardsCompatibleDescriptor.fromFilename("dir/ks-cf-he-123-Index.db");
        assertEquals("ks", desc.ksname);
        assertEquals("cf", desc.cfname);
        assertEquals(false, desc.temporary);
        assertEquals("he", desc.version.toString());
        assertEquals(123, desc.generation);

        desc = BackwardsCompatibleDescriptor.fromFilename("dir/tmp-tmp-he-123-Index.db");
        assertEquals("tmp", desc.ksname);
        assertEquals("tmp", desc.cfname);
        assertEquals(false, desc.temporary);
        assertEquals("he", desc.version.toString());
        assertEquals(123, desc.generation);

        desc = BackwardsCompatibleDescriptor.fromFilename("dir/tmp-tmp-tmp-ic-123-Index.db");
        assertEquals("tmp", desc.ksname);
        assertEquals("tmp", desc.cfname);
        assertEquals(true, desc.temporary);
        assertEquals("ic", desc.version.toString());
        assertEquals(123, desc.generation);

        desc = BackwardsCompatibleDescriptor.fromFilename("dir/tmp-cf-tmp-ic-123-Index.db");
        assertEquals("tmp", desc.ksname);
        assertEquals("cf", desc.cfname);
        assertEquals(true, desc.temporary);
        assertEquals("ic", desc.version.toString());
        assertEquals(123, desc.generation);
    }

    @Test
    public void testFilenameFor() {
        Descriptor desc = BackwardsCompatibleDescriptor.fromFilename("dir/ks-cf-ic-123-Data.db");
        assertEquals("dir/ks-cf-ic-123-Data.db", desc.filenameFor(Component.DATA));

        desc = BackwardsCompatibleDescriptor.fromFilename("ks/cf-h-123-Data.db");
        assertEquals("ks/cf-h-123-Data.db", desc.filenameFor(Component.DATA));
    }

}
