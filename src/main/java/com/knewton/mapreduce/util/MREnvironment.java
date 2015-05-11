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
package com.knewton.mapreduce.util;

import com.google.common.base.Preconditions;

public enum MREnvironment {

    LOCAL("local"),
    HADOOP_MAPREDUCE("HadoopMapReduce"),
    EMR("EMR");

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
     *            The string value representation of the environment
     * @return The Hadoop environment.
     */
    public static MREnvironment fromValue(String strVal) {
        Preconditions.checkNotNull(strVal, "Parameter cannot be null");
        for (MREnvironment mre : MREnvironment.values()) {
            if (mre.getValue().equalsIgnoreCase(strVal)) {
                return mre;
            }
        }
        throw new IllegalArgumentException(String.format("Cannot determine type from %s", strVal));
    }

}
