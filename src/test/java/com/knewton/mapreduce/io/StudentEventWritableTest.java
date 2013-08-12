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

import static org.junit.Assert.*;

import com.knewton.thrift.StudentEvent;
import com.knewton.thrift.StudentEventData;
import com.knewton.mapreduce.io.StudentEventWritable;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;

public class StudentEventWritableTest {

    private TDeserializer deserializer;

    @Before
    public void setUp() throws Exception {
        TProtocolFactory factory = new TCompactProtocol.Factory();
        this.deserializer = new TDeserializer(factory);
    }

    @Test
    public void testSerialization() throws DecoderException, TException,
            IOException {
        byte[] studentEventDataBytes = Hex.decodeHex(
                eventDataString.toCharArray());
        StudentEventData studentEventData = new StudentEventData();
        deserializer.deserialize(studentEventData, studentEventDataBytes);
        StudentEvent studentEvent = new StudentEvent(1, studentEventData);
        StudentEventWritable sew = new StudentEventWritable(studentEvent, 10L);

        ByteArrayOutputStream byteArrayOutputStream =
                new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(byteArrayOutputStream);
        sew.write(dout);

        StudentEventWritable deserializedWritable = new StudentEventWritable();
        InputStream is = new ByteArrayInputStream(
                byteArrayOutputStream.toByteArray());
        DataInputStream dis = new DataInputStream(is);
        deserializedWritable.readFields(dis);
        assertEquals(sew.getStudentEvent(),
                deserializedWritable.getStudentEvent());
        assertEquals(sew.getTimestamp(),
                deserializedWritable.getTimestamp());
    }

    private static final String eventDataString =
            "169ad7a6d4e5d2abec4d16bae2bed5e04f181ee7aca9e188a5ec8d90eabe83ec" +
                    "8594ee9bb2e6aaaeefa182e39184e7a3941508181dd480e28c98e692" +
                    "8de29a9fe7bc9bebb380ec99bfe480b0e990bde7b0a9181de29291eb" +
                    "86bfe582b3e0b686e2879cd79ce2838ae3a2aae3a9bee7949000";
}
