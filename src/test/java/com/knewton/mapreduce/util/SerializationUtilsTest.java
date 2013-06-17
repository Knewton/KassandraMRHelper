package com.knewton.mapreduce.util;

import static org.junit.Assert.*;

import com.knewton.thrift.StudentEventData;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.transport.TTransportException;
import org.junit.Test;

public class SerializationUtilsTest {

    /**
     * Test if a deserializer can be instantiated correctly from a Hadoop conf property.
     * 
     * @throws TException
     * @throws DecoderException
     */
    @Test
    public void testDeserializerInstantiation() throws TException,
            DecoderException {
        Configuration conf = new Configuration();
        conf.set(SerializationUtils.SERIALIZATION_FACTORY_PARAMETER,
                TCompactProtocol.Factory.class.getName());
        TDeserializer deserializer =
                SerializationUtils.getDeserializerFromContext(conf);
        assertNotNull(deserializer);

        StudentEventData sed = new StudentEventData();
        byte[] studentEventDataBytes = Hex.decodeHex(
                eventDataString.toCharArray());
        deserializer.deserialize(sed, studentEventDataBytes);
        assertNotNull(sed.getBook());
        assertNotNull(sed.getCourse());
        assertNotNull(sed.getType());
        assertNotEquals(0L, sed.getScore());
        assertNotEquals(0L, sed.getStudentId());
        assertNotEquals(0L, sed.getTimestamp());
    }

    /**
     * Test if a deserializer can be instantiated correctly from a Hadoop conf with the default
     * property.
     * 
     * @throws TException
     * @throws DecoderException
     */
    @Test
    public void testDefaultDeserializerInstantiation() throws TException,
            DecoderException {
        TDeserializer deserializer =
                SerializationUtils.getDeserializerFromContext(new Configuration());
        assertNotNull(deserializer);

        StudentEventData sed = new StudentEventData();
        byte[] studentEventDataBytes = Hex.decodeHex(
                eventDataString.toCharArray());
        deserializer.deserialize(sed, studentEventDataBytes);
        assertNotNull(sed.getBook());
        assertNotNull(sed.getCourse());
        assertNotNull(sed.getType());
        assertNotEquals(0L, sed.getScore());
        assertNotEquals(0L, sed.getStudentId());
        assertNotEquals(0L, sed.getTimestamp());
    }

    /**
     * Try to get the wrong deserializer from the conf and tries to deserialize some event data with
     * the wrong TProtocol.
     * 
     * @throws DecoderException
     * @throws TException
     */
    @Test(expected = TTransportException.class)
    public void testDeserializerInstantiationWithWrongDeserializer() throws
            DecoderException, TException {
        Configuration conf = new Configuration();
        conf.set(SerializationUtils.SERIALIZATION_FACTORY_PARAMETER,
                TBinaryProtocol.Factory.class.getName());
        TDeserializer deserializer =
                SerializationUtils.getDeserializerFromContext(conf);
        assertNotNull(deserializer);

        StudentEventData sed = new StudentEventData();
        byte[] studentEventDataBytes = Hex.decodeHex(
                eventDataString.toCharArray());
        deserializer.deserialize(sed, studentEventDataBytes);
    }

    private static final String eventDataString =
            "169ad7a6d4e5d2abec4d16bae2bed5e04f181ee7aca9e188a5ec8d90eabe83ec" +
                    "8594ee9bb2e6aaaeefa182e39184e7a3941508181dd480e28c98e692" +
                    "8de29a9fe7bc9bebb380ec99bfe480b0e990bde7b0a9181de29291eb" +
                    "86bfe582b3e0b686e2879cd79ce2838ae3a2aae3a9bee7949000";

}
