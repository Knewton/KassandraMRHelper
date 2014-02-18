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

import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolFactory;

import java.lang.reflect.InvocationTargetException;

public class SerializationUtils {

    public static final String SERIALIZATION_FACTORY_PARAMETER =
            "com.knewton.thrift.serialization.protocol";

    public static final String SERIALIZATION_FACTORY_PARAMETER_DEFAULT =
            TCompactProtocol.Factory.class.getName();

    public static TDeserializer getDeserializerFromContext(Configuration conf) {
        String protocolFactoryStr = conf.get(SERIALIZATION_FACTORY_PARAMETER,
                SERIALIZATION_FACTORY_PARAMETER_DEFAULT);
        TProtocolFactory protocolFactory;
        try {
            protocolFactory = (TProtocolFactory) Class.
                    forName(protocolFactoryStr).getConstructor().newInstance();
        } catch (InstantiationException e) {
            throw new SerializationConfigurationException(String.format(
                    "Could not instantiate deserializer protocol factory for class name %s.",
                    protocolFactoryStr), e);
        } catch (IllegalAccessException e) {
            throw new SerializationConfigurationException(String.format(
                    "Default constructor for class %s is not accessible.",
                    protocolFactoryStr), e);
        } catch (InvocationTargetException e) {
            throw new SerializationConfigurationException(String.format(
                    "Could not instantiate class %s.", protocolFactoryStr), e);
        } catch (NoSuchMethodException e) {
            throw new SerializationConfigurationException(String.format(
                    "No default constructor for class %s.",
                    protocolFactoryStr), e);
        } catch (ClassNotFoundException e) {
            throw new SerializationConfigurationException(String.format(
                    "Could not find class %s.", protocolFactoryStr), e);
        }
        return new TDeserializer(protocolFactory);
    }

    public static class SerializationConfigurationException extends RuntimeException {

        private static final long serialVersionUID = 1368581102967640365L;

        public SerializationConfigurationException(String message) {
            super(message);
        }

        public SerializationConfigurationException(String message, Exception e) {
            super(message, e);
        }
    }

}
