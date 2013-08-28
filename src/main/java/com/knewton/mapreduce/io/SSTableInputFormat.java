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

import com.knewton.mapreduce.SSTableRecordReader;
import com.knewton.mapreduce.io.sstable.BackwardsCompatibleDescriptor;

import org.apache.cassandra.io.sstable.SSTable;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import javax.annotation.Nullable;

/**
 * Input format for reading cassandra SSTables. When given an input directory, it expands all
 * subdirectories and identifies SSTable data files that can be used as input.
 * 
 * @param <K>
 * @param <V>
 */
public abstract class SSTableInputFormat<K, V> extends FileInputFormat<K, V> {

    private static final String COLUMN_FAMILY_NAME_PARAMETER =
            "com.knewton.inputformat.cassandra.columnfamily";
    private static final Logger LOG =
            LoggerFactory.getLogger(SSTableInputFormat.class);

    /**
     * Make SSTables not splittable for now so send the entire file to each record writer.
     * 
     * @param context
     *            The job context
     * @param path
     * @return Returns false.
     */
    @Override
    public boolean isSplitable(JobContext context, Path path) {
        return false;
    }

    /**
     * Expands all directories passed as input and keeps only valid data tables.
     * 
     * @param job
     * @return A list of all the data tables found under the input directories.
     * @throws IOException
     */
    @Override
    protected List<FileStatus> listStatus(JobContext job) throws IOException {
        List<FileStatus> files = super.listStatus(job);
        String operatingCF =
                job.getConfiguration().get(COLUMN_FAMILY_NAME_PARAMETER);
        DataTablePathFilter dataTableFilter;
        if (operatingCF != null) {
            dataTableFilter = new DataTablePathFilter(operatingCF);
        } else {
            dataTableFilter = new DataTablePathFilter();
        }
        for (int i = 0; i < files.size(); i++) {
            FileStatus file = files.get(i);
            // Expand if directory
            if (file.isDir()) {
                Path p = file.getPath();
                LOG.info("Expanding {}", p);
                FileSystem fs = p.getFileSystem(job.getConfiguration());
                FileStatus[] children = fs.listStatus(p);
                files.addAll(i + 1, Arrays.asList(children));
            }
            if (!dataTableFilter.accept(file.getPath())) {
                LOG.info("Removing: " + file.getPath().toString());
                files.remove(i);
                i--;
            }
        }

        return files;
    }

    /**
     * Comparator class name for columns.
     * 
     * @param name
     * @param job
     */
    public static void setComparatorClass(String name, Job job) {
        job.getConfiguration().set(
                SSTableRecordReader.COLUMN_COMPARATOR_PARAMETER, name);
    }

    /**
     * This is not required if the column family type is standard.
     * 
     * @param name
     * @param job
     */
    public static void setSubComparatorClass(String name, Job job) {
        job.getConfiguration().set(
                SSTableRecordReader.COLUMN_SUBCOMPARATOR_PARAMETER, name);
    }

    /**
     * Partitioner for decorating keys.
     * 
     * @param name
     * @param job
     */
    public static void setPartitionerClass(String name, Job job) {
        job.getConfiguration().set(
                SSTableRecordReader.PARTITIONER_PARAMETER, name);
    }

    /**
     * Column family type needs to be set if the column family type is Super.
     * 
     * @param name
     * @param job
     */
    public static void setColumnFamilyType(String name, Job job) {
        job.getConfiguration().set(
                SSTableRecordReader.COLUMN_FAMILY_TYPE_PARAMETER, name);
    }

    /**
     * Set the name of the column family to read. This is optional. If not set all the datatables
     * under the given input directory will be collected and processed.
     * 
     * @param name
     * @param job
     */
    public static void setColumnFamilyName(String name, Job job) {
        job.getConfiguration().set(
                COLUMN_FAMILY_NAME_PARAMETER, name);
    }

    /**
     * Custom path filter for SSTables.
     * 
     */
    public static class DataTablePathFilter implements PathFilter {

        @Nullable
        private String operatingCF;

        public DataTablePathFilter(@Nullable String operatingCF) {
            this.operatingCF = operatingCF;
        }

        public DataTablePathFilter() {
            this(null);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean accept(Path path) {
            // ignore if 1) path is null, 2) it's not a data file or 3) if it's a temporary file.
            if (path == null || !path.getName().endsWith(SSTable.COMPONENT_DATA)
                    || BackwardsCompatibleDescriptor.fromFilename(path.toString()).temporary) {
                return false;
            } else if (operatingCF == null) {
                return true;
            } else {
                return BackwardsCompatibleDescriptor.fromFilename(path.toString()).cfname
                        .equals(operatingCF);
            }
        }

    }

}
