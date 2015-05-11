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

import com.knewton.mapreduce.constant.PropertyConstants;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TCompactProtocol.Factory;
import org.apache.thrift.protocol.TProtocolFactory;

public class SerializationUtils {

    public static final Class<Factory> SERIALIZATION_FACTORY_PARAMETER_DEFAULT =
        TCompactProtocol.Factory.class;

    public static TDeserializer getDeserializerFromContext(Configuration conf) {
        Class<? extends TProtocolFactory> protocolFactoryClass =
            conf.getClass(PropertyConstants.SERIALIZATION_FACTORY_PARAMETER.txt,
                          SERIALIZATION_FACTORY_PARAMETER_DEFAULT, TProtocolFactory.class);
        TProtocolFactory protocolFactory = ReflectionUtils.newInstance(protocolFactoryClass, conf);
        return new TDeserializer(protocolFactory);
    }

}
