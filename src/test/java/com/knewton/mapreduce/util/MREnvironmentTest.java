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
package com.knewton.mapreduce.util;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests {@link MREnvironment}
 *
 * @author giannis
 *
 */
public class MREnvironmentTest {

    @Test
    public void testFromValue() {
        MREnvironment mre = MREnvironment.fromValue("HadoopMapReduce");
        assertEquals(mre, MREnvironment.HADOOP_MAPREDUCE);
        mre = MREnvironment.fromValue("local");
        assertEquals(mre, MREnvironment.LOCAL);
        mre = MREnvironment.fromValue("EMR");
        assertEquals(mre, MREnvironment.EMR);
    }

    @Test(expected = NullPointerException.class)
    public void testFromValueWithNull() {
        MREnvironment.fromValue(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFromValueIllegalString() {
        MREnvironment.fromValue("invalid");
    }

}
