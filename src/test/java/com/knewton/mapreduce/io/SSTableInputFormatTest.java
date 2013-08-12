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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import mockit.Mock;
import mockit.MockUp;

import org.apache.cassandra.db.IColumn;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SSTableInputFormatTest {

    private static final int NUM_TABLES = 4;
    private static final int NUM_TOKENS = 3;
    private List<FileStatus> expectedResult;
    private Map<String, List<FileStatus>> pathHierarchy;

    /**
     * Tests to see if when given an input directory the {@link SSTableInputFormat} correctly
     * expands all sub directories and picks up all the data tables.
     * 
     * @throws IOException
     */
    @Test
    public void testSSTableInputFormatNoColumnFamilyName() throws IOException {
        setUpPaths(false, "col_fam");
        List<FileStatus> result = testSSTableInputFormat(new Configuration());
        assertEquals(NUM_TABLES * NUM_TOKENS, result.size());
        assertEquals(result, expectedResult);
    }

    /**
     * Tests to see if when given an input directory the {@link SSTableInputFormat} correctly
     * expands all sub directories and picks up all the data tables corresponding to a specific
     * column family.
     * 
     * @throws IOException
     */
    @Test
    public void testSSTableInputFormatColumnFamilyName() throws IOException {
        String cfName = "col_fam";
        setUpPaths(true, cfName);
        Job job = new Job(new Configuration());
        SSTableInputFormat.setColumnFamilyName(cfName, job);
        List<FileStatus> result = testSSTableInputFormat(job.getConfiguration());
        assertEquals(NUM_TABLES * NUM_TOKENS, result.size());
        assertEquals(result, expectedResult);
    }

    /**
     * Helper method for setting up all the mock files in the FS. Calls list status in the input
     * format and returns the result.
     * 
     * @param conf
     * @return
     * @throws IOException
     */
    private List<FileStatus> testSSTableInputFormat(Configuration conf)
            throws IOException {
        new MockUp<FileInputFormat<ByteBuffer, IColumn>>() {
            @Mock
            List<FileStatus> listStatus(JobContext job) throws IOException {
                List<FileStatus> files = Lists.newArrayList();
                files.add(new FileStatus(0, true, 0, 0, 0, new Path("/input/")));
                return files;
            }

        };
        new MockUp<LocalFileSystem>() {
            @Mock
            FileStatus[] listStatus(Path path) throws IOException {
                List<FileStatus> subPath = pathHierarchy.get(path.toString());
                if (subPath != null) {
                    return subPath.toArray(new FileStatus[0]);
                } else {
                    return new FileStatus[0];
                }
            }

        };
        SSTableColumnInputFormat inputFormat = new SSTableColumnInputFormat();
        return inputFormat.listStatus(
                new JobContext(conf, new JobID()));
    }

    /**
     * Setup a mock path hierarchy in the filesystem. It includes <code>NUM_TOKENS</code> tokens
     * with three subdirectories representing tokens, each token having <code>NUM_TOKENS</code>
     * sstables.
     * 
     * @param cfName
     * @throws Exception
     */
    private void setUpPaths(boolean multiCFPath, String cfName) {
        expectedResult = new ArrayList<FileStatus>(NUM_TABLES * NUM_TOKENS);
        pathHierarchy = Maps.newHashMap();
        List<FileStatus> tokenPaths = new ArrayList<FileStatus>(NUM_TOKENS);
        int mult = 1;
        if (multiCFPath) {
            mult = 2;
        }
        for (int i = 1; i <= NUM_TOKENS; i++) {
            tokenPaths.add(getFileStatus("/input/token" + i, true));
            List<FileStatus> childPaths = new ArrayList<FileStatus>(5 * mult);
            for (int j = i * 5; j < i * 5 + NUM_TABLES; j++) {
                createChildNodes(childPaths, cfName, i, j, true);
                if (multiCFPath) {
                    createChildNodes(childPaths, "bad_col_fam", i, j, false);
                }
            }
            pathHierarchy.put("/input/token" + i, childPaths);
        }
        pathHierarchy.put("/input", tokenPaths);
    }

    /**
     * Create all the sstables and add them as children. Also add the data table in the expected
     * results if necessary.
     * 
     * @param childPaths
     * @param cfName
     * @param i
     * @param j
     * @param addToExpectedResult
     */
    private void createChildNodes(List<FileStatus> childPaths, String cfName,
            int i, int j, boolean addToExpectedResult) {
        FileStatus dataFileStatus = getFileStatus(
                "/input/token" + i + "/" + cfName + "-hg-" + j + "-Data.db",
                false);
        if (addToExpectedResult) {
            expectedResult.add(dataFileStatus);
        }
        childPaths.add(dataFileStatus);
        childPaths.add(getFileStatus("/input/token" + i + "/" + cfName + "-hg-"
                + j + "-Index.db", false));
        childPaths.add(getFileStatus("/input/token" + i + "/" + cfName + "-hg-"
                + j + "-Statistics.db", false));
        childPaths.add(getFileStatus("/input/token" + i + "/" + cfName + "-hg-"
                + j + "-CompressionInfo.db", false));
        childPaths.add(getFileStatus("/input/token" + i + "/" + cfName + "-hg-"
                + j + "-Filter.db", false));
    }

    /**
     * Builds a FileStatus objects, not really representing anything on disk.
     * 
     * @param inputPath
     * @param directory
     * @return
     */
    private FileStatus getFileStatus(String inputPath, boolean directory) {
        return new FileStatus(0, directory, 1, 1, 1, new Path(inputPath));
    }

}
