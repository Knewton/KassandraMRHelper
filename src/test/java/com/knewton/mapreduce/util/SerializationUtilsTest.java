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

import com.knewton.mapreduce.constant.PropertyConstants;

import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public class SerializationUtilsTest {

    /**
     * Test if a deserializer can be instantiated correctly from a Hadoop conf property.
     */
    @Test
    public void testGetDeserializerFromConf() throws Exception {
        Configuration conf = new Configuration(false);
        conf.set(PropertyConstants.SERIALIZATION_FACTORY_PARAMETER.txt,
                 TCompactProtocol.Factory.class.getName());
        TDeserializer deserializer = SerializationUtils.getDeserializerFromConf(conf);
        assertNotNull(deserializer);
    }

}
