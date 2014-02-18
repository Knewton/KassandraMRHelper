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
package com.knewton.mapreduce.io.sstable;

import com.google.common.base.Splitter;

import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.utils.Pair;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.util.Iterator;

import static org.apache.cassandra.io.sstable.Component.separator;

/**
 * This class is here to add backwards compatibility to sstables from older cassandra versions. The
 * latest version currently supported is {@link Version#CURRENT}.
 * <p/>
 * Cassandra 0.x-1.0 file format:
 * &lt;cfname&gt;-[tmp-][&lt;version&gt;-]&lt;gen&gt;-&lt;component&gt; <br/>
 * Cassandra 1.1-1.2 file format:
 * &lt;ksname&gt;-&lt;cfname&gt;-[tmp-][&lt;version&gt;-]&lt;gen&gt;-&lt;component&gt;
 *
 * @author giannis
 *
 */
public class BackwardsCompatibleDescriptor extends Descriptor {

    public static final int NUM_SEPARATORS = 4;

    public BackwardsCompatibleDescriptor(Descriptor descriptor) {
        this(descriptor.version, descriptor.directory, descriptor.ksname, descriptor.cfname,
                descriptor.generation, descriptor.temporary);
    }

    public BackwardsCompatibleDescriptor(Version version, File directory, String ksname,
            String cfname, int generation, boolean temp) {
        super(version, directory, ksname, cfname, generation, temp);
    }

    /**
     * Backwards compatible filenameFor method that removes the keyspace name for older sstable
     * versions.
     */
    @Override
    public String filenameFor(String suffix) {
        String filename = super.filenameFor(suffix);
        if (version.hasAncestors) {
            return filename;
        } else {
            return filename.replace(ksname + separator, "");
        }
    }

    /**
     * Implementation of {@link Descriptor#fromFilename(String)} that is backwards compatible with
     * older sstables
     *
     * @param filename
     * @return A descriptor for the sstable
     */
    public static Descriptor fromFilename(String filename) {
        File file = new File(filename);
        return fromFilename(file.getParentFile(), file.getName()).left;
    }

    /**
     * Implementation of {@link Descriptor#fromFilename(File, String)} that is backwards compatible
     * with older sstables
     *
     * @param directory
     * @param name
     * @return A descriptor for the sstable
     */
    public static Pair<Descriptor, String> fromFilename(File directory, String name) {
        Iterator<String> iterator = Splitter.on(separator).split(name).iterator();
        iterator.next();
        String tempFlagMaybe = iterator.next();
        String versionMaybe = iterator.next();
        String generationMaybe = iterator.next();
        Pair<Descriptor, String> dsPair;
        if (tempFlagMaybe.equals(SSTable.TEMPFILE_MARKER) && generationMaybe.matches("\\d+")
                && !new Version(versionMaybe).hasAncestors) {
            // old sstable file with temp flag.
            dsPair = Descriptor.fromFilename(directory, directory.getName() + separator + name);
        } else if (versionMaybe.equals(SSTable.TEMPFILE_MARKER)) {
            // new sstable file with temp flag.
            dsPair = Descriptor.fromFilename(directory, name);
        } else if (StringUtils.countMatches(name, String.valueOf(separator)) < NUM_SEPARATORS) {
            // old sstable file with no temp flag.
            dsPair = Descriptor.fromFilename(directory, directory.getName() + separator + name);
        } else {
            // new sstable file with no temp flag.
            dsPair = Descriptor.fromFilename(directory, name);
        }
        // cast so that Pair doens't complain.
        return Pair.create((Descriptor) new BackwardsCompatibleDescriptor(dsPair.left),
                dsPair.right);
    }
}
