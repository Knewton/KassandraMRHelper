package com.knewton.mapreduce.util;

/**
 * Copyright (c) 2013 Knewton
 * 
 * Dual licensed under: MIT: http://www.opensource.org/licenses/mit-license.php GPLv3:
 * http://www.opensource.org/licenses/gpl-3.0.html.
 * 
 */
public enum MREnvironment {
    LOCAL("local"),
    HADOOP_MAPREDUCE("HadoopMapReduce"),
    EMR("EMR");

    public static String MAPREDUCE_ENVIRONMENT_PARAMETER =
            "com.knewton.mapreduce.environment";

    private String value;

    private MREnvironment(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return value;
    }

    public String getValue() {
        return value;
    }

    /**
     * Gets an enum type from a string value.
     * 
     * @param strVal
     * @return
     */
    public static MREnvironment fromValue(String strVal) {
        if (strVal != null) {
            for (MREnvironment mre : MREnvironment.values()) {
                if (mre.getValue().equalsIgnoreCase(strVal)) {
                    return mre;
                }
            }
        }
        throw new IllegalArgumentException(String.format(
                "Cannot determine type from %s", strVal));
    }

}
