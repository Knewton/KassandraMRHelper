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

import com.knewton.mapreduce.constant.PropertyConstants;
import com.knewton.mapreduce.io.sstable.BackwardsCompatibleDescriptor;

import com.google.common.collect.Lists;

import org.apache.cassandra.io.sstable.SSTable;
import org.apache.hadoop.conf.Configuration;
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

    public static final String FULL_PRIAM_BACKUP_DIR_NAME = "SNAP";
    public static final String INCREMENTAL_PRIAM_BACKUP_DIR_NAME = "SST";
    public static final String COMPLETE_BACKUP_INDICATOR_DIR_NAME = "META";
    private static final Logger LOG = LoggerFactory.getLogger(SSTableInputFormat.class);

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
     * @return A list of all the data tables found under the input directories.
     */
    @Override
    protected List<FileStatus> listStatus(JobContext job) throws IOException {
        Configuration conf = job.getConfiguration();
        List<FileStatus> files = super.listStatus(job);
        DataTablePathFilter dataTableFilter = getDataTableFilter(conf);
        files = cleanUpBackupDir(files);
        for (int i = 0; i < files.size(); i++) {
            FileStatus file = files.get(i);
            Path p = file.getPath();
            // Expand if directory
            if (file.isDirectory() && p != null) {
                LOG.info("Expanding {}", p);
                FileSystem fs = p.getFileSystem(conf);
                FileStatus[] children = fs.listStatus(p);
                List<FileStatus> listChildren = Lists.newArrayList(children);
                listChildren = cleanUpBackupDir(listChildren);
                files.addAll(i + 1, listChildren);
            }
            if (!dataTableFilter.accept(file.getPath())) {
                LOG.info("Removing {}", file.getPath());
                files.remove(i);
                i--;
            }
        }
        return files;
    }

    /**
     * Remove INCREMENTAL_PRIAM_BACKUP_DIR_NAME if found FULL_PRIAM_BACKUP_DIR_NAME
     *
     * @param files
     *            - a list of FileStus
     * @return The list with INCREMENTAL_PRIAM_BACKUP_DIR_NAME removed if FULL_PRIAM_BACKUP_DIR_NAME
     *         found
     */
    private List<FileStatus> cleanUpBackupDir(List<FileStatus> files) {
        boolean foundFullBackupDir = false;
        for (FileStatus nestedDirStatus : files) {
            String nestedDirName = nestedDirStatus.getPath().getName();
            if (nestedDirName.equals(FULL_PRIAM_BACKUP_DIR_NAME)) {
                foundFullBackupDir = true;
            }
        }
        if (foundFullBackupDir) {
            for (int i = 0; i < files.size(); i++) {
                FileStatus nestedDirStatus = files.get(i);
                String nestedDirName = nestedDirStatus.getPath().getName();
                if (nestedDirName.equals(INCREMENTAL_PRIAM_BACKUP_DIR_NAME)) {
                    LOG.info("Removing {}", nestedDirStatus.getPath());
                    files.remove(i);
                    i--;
                }
            }
        }
        return files;
    }

    /**
     * Configure the DataTableFilter and return it.
     *
     * @return The DataTableFilter.
     */
    private DataTablePathFilter getDataTableFilter(Configuration conf) {
        String operatingCF = conf.get(PropertyConstants.COLUMN_FAMILY_NAME.txt);
        return new DataTablePathFilter(operatingCF);
    }

    /**
     * Comparator class name for columns.
     *
     * @param value
     *            The value of the property
     * @param job
     *            The current job
     */
    public static void setComparatorClass(String value, Job job) {
        job.getConfiguration().set(PropertyConstants.COLUMN_COMPARATOR.txt, value);
    }

    /**
     * This is not required if the column family type is standard.
     *
     * @param value
     *            The value of the property
     * @param job
     *            The current job
     */
    public static void setSubComparatorClass(String value, Job job) {
        job.getConfiguration().set(PropertyConstants.COLUMN_SUBCOMPARATOR.txt, value);
    }

    /**
     * Partitioner for decorating keys.
     *
     * @param value
     *            The value of the property
     * @param job
     *            The current job
     */
    public static void setPartitionerClass(String value, Job job) {
        job.getConfiguration().set(PropertyConstants.PARTITIONER.txt, value);
    }

    /**
     * Column family type needs to be set if the column family type is Super.
     *
     * @param value
     *            The value of the property
     * @param job
     *            The current job
     */
    public static void setColumnFamilyType(String value, Job job) {
        job.getConfiguration().set(PropertyConstants.COLUMN_FAMILY_TYPE.txt, value);
    }

    /**
     * Set the name of the column family to read. This is optional. If not set all the datatables
     * under the given input directory will be collected and processed.
     *
     * @param value
     *            The value of the property
     * @param job
     *            The current job
     */
    public static void setColumnFamilyName(String value, Job job) {
        job.getConfiguration().set(PropertyConstants.COLUMN_FAMILY_NAME.txt, value);
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
