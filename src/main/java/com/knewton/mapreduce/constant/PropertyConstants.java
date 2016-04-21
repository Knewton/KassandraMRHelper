package com.knewton.mapreduce.constant;

public enum PropertyConstants {

    /**
     * The Keyspace Name for a data run
     */
    KEYSPACE_NAME("com.knewton.inputformat.cassandra.keyspace"),
    /**
     * Column family type needs to be set if the column family type is Super.
     */
    COLUMN_FAMILY_TYPE("com.knewton.cassandra.cftype"),
    /**
     * Comparator class name for columns.
     */
    COLUMN_COMPARATOR("com.knewton.cassandra.column.comparator"),
    /**
     * A nonstandard comparator requires a subcomparator
     */
    COLUMN_SUBCOMPARATOR("com.knewton.cassandra.column.subcomparator"),
    /**
     * Partitioner for decorating keys.
     */
    PARTITIONER("com.knewton.partitioner"),
    /**
     * True if the columns are sparse, false if they're dense.
     */
    SPARSE_COLUMN("com.knewton.column.sparse"),
    /**
     * Boolean variable. Set true if compression enabled.
     */
    COMPRESSION_ENABLED("com.knewton.cassandra.backup.compression"),
    /**
     * Buffer size for decompression operation.
     */
    DECOMPRESS_BUFFER("com.knewton.cassandra.backup.compress.buffersize"),
    /**
     * Start and end dates for student events processing
     */
    START_DATE("com.knewton.studentevents.date.start"),
    END_DATE("com.knewton.studentevents.date.end"),
    /**
     * The Column Family Name for a data run
     */
    COLUMN_FAMILY_NAME("com.knewton.inputformat.cassandra.columnfamily"),
    /**
     * MapReduce run environment. Should be one of the enum values in MREnvironment.java
     */
    MAPREDUCE_ENVIRONMENT("com.knewton.mapreduce.environment"),
    /**
     * Name of serialization factory class
     */
    SERIALIZATION_FACTORY_PARAMETER("com.knewton.thrift.serialization.protocol");

    public final String txt;

    private PropertyConstants(String propertyName) {
        this.txt = propertyName;
    }
}
