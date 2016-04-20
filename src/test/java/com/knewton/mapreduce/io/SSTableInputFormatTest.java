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
package com.knewton.mapreduce.io;

import com.knewton.mapreduce.constant.PropertyConstants;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class SSTableInputFormatTest {

    private static final int NUM_TABLES = 4;
    private static final int NUM_TOKENS = 3;
    private static final int NUM_COLUMN_FAMILIES = 2;

    /**
     * Tests to see if when given an input directory the {@link SSTableInputFormat} correctly
     * expands all sub directories and picks up all the data tables.
     */
    @Test
    public void testListStatusNoColumnFamilyName() throws IOException {
        Configuration conf = new Configuration(false);
        List<FileStatus> result = testListStatus(conf, "./src/test/resources/input/");
        assertEquals(NUM_TABLES * NUM_TOKENS * NUM_COLUMN_FAMILIES, result.size());
    }

    /**
     * Tests to see if when given an input directory the {@link SSTableInputFormat} correctly
     * expands all sub directories and picks up all the data tables corresponding to a specific
     * column family.
     */
    @Test
    public void testListStatusWithColumnFamilyName() throws IOException {
        String cfName = "col_fam";
        Job job = Job.getInstance(new Configuration(false));
        Configuration conf = job.getConfiguration();
        SSTableInputFormat.setColumnFamilyName(cfName, job);
        List<FileStatus> result = testListStatus(conf, "./src/test/resources/input/");
        assertEquals(NUM_TABLES * NUM_TOKENS, result.size());
    }

    /**
     * Tests to see if when given an input directory the {@link SSTableInputFormat} correctly
     * expands all sub directories and picks up all the data tables when a SNAP directory exists.
     * The SST tables should be skipped.
     */
    @Test
    public void testListStatusNoColumnFamilyNameSkipSST() throws Exception {
        Configuration conf = new Configuration(false);
        List<FileStatus> result = testListStatus(conf, "./src/test/resources/backup_input");
        assertEquals(NUM_TABLES * NUM_COLUMN_FAMILIES, result.size());
    }

    /**
     * Tests to see if when given an input directory the {@link SSTableInputFormat} correctly
     * expands all sub directories and picks up all the data tables corresponding to a specific
     * column family when a SNAP directory exists. The SST tables should be skipped.
     */
    @Test
    public void testListStatusWithColumnFamilyNameSkipSST() throws Exception {
        Job job = Job.getInstance(new Configuration(false));
        Configuration conf = job.getConfiguration();
        SSTableInputFormat.setColumnFamilyName("col_fam", job);
        List<FileStatus> result = testListStatus(conf, "./src/test/resources/backup_input");
        assertEquals(NUM_TABLES, result.size());
    }

    @Test
    public void testIsSplittable() {
        SSTableColumnInputFormat inputFormat = new SSTableColumnInputFormat();
        assertFalse(inputFormat.isSplitable(null, null));
    }

    @Test
    public void testSetComparatorClass() throws Exception {
        Job job = Job.getInstance(new Configuration(false));
        Configuration conf = job.getConfiguration();
        String comparator = "my_comparator";
        SSTableInputFormat.setComparatorClass(comparator, job);
        assertEquals(comparator, conf.get(PropertyConstants.COLUMN_COMPARATOR.txt));
    }

    @Test
    public void testSetSubComparatorClass() throws Exception {
        Job job = Job.getInstance(new Configuration(false));
        Configuration conf = job.getConfiguration();
        String subComparator = "my_subcomparator";
        SSTableInputFormat.setSubComparatorClass(subComparator, job);
        assertEquals(subComparator, conf.get(PropertyConstants.COLUMN_SUBCOMPARATOR.txt));
    }

    @Test
    public void testPartitionerClass() throws Exception {
        Job job = Job.getInstance(new Configuration(false));
        Configuration conf = job.getConfiguration();
        String partitioner = "my_partitioner";
        SSTableInputFormat.setPartitionerClass(partitioner, job);
        assertEquals(partitioner, conf.get(PropertyConstants.PARTITIONER.txt));
    }

    @Test
    public void testColumnFamilyType() throws Exception {
        Job job = Job.getInstance(new Configuration(false));
        Configuration conf = job.getConfiguration();
        String cfType = "my_cftype";
        SSTableInputFormat.setColumnFamilyType(cfType, job);
        assertEquals(cfType, conf.get(PropertyConstants.COLUMN_FAMILY_TYPE.txt));
    }

    @Test
    public void testSetColumnFamilyName() throws Exception {
        Job job = Job.getInstance(new Configuration(false));
        Configuration conf = job.getConfiguration();
        String cfName = "my_cfName";
        SSTableInputFormat.setColumnFamilyName(cfName, job);
        assertEquals(cfName, conf.get(PropertyConstants.COLUMN_FAMILY_NAME.txt));
    }

    @Test
    public void testSetKeyspaceName() throws Exception {
        Job job = Job.getInstance(new Configuration(false));
        Configuration conf = job.getConfiguration();
        String keyspaceName = "my_keyspaceName";
        SSTableInputFormat.setKeyspaceName(keyspaceName, job);
        assertEquals(keyspaceName, conf.get(PropertyConstants.KEYSPACE_NAME.txt));
    }

    /**
     * Helper method for setting up all the mock files in the FS. Calls list status in the input
     * format and returns the result.
     */
    private List<FileStatus> testListStatus(Configuration conf, String dirs) throws IOException {
        JobID jobId = new JobID();
        conf.set(FileInputFormat.INPUT_DIR, dirs);
        JobContextImpl job = new JobContextImpl(conf, jobId);
        SSTableColumnInputFormat inputFormat = new SSTableColumnInputFormat();
        return inputFormat.listStatus(job);
    }

}
