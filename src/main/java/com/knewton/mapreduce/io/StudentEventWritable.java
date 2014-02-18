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

import com.knewton.thrift.StudentEvent;

import org.apache.hadoop.io.Writable;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;

/**
 * Wrapper class for StudentEvents that is Hadoop writable so that it can be used in the reducer.
 *
 */
public class StudentEventWritable implements Writable, Cloneable {

    private StudentEvent studentEvent;
    private final TSerializer serializer;
    private final TDeserializer deserializer;
    private long timestamp;

    public StudentEventWritable() {
        this(null);
    }

    public StudentEventWritable(StudentEvent studentEvent) {
        this(studentEvent, 0);
    }

    public StudentEventWritable(StudentEvent studentEvent, long timestamp) {
        TBinaryProtocol.Factory protocolFactory = new TBinaryProtocol.Factory();
        this.serializer = new TSerializer(protocolFactory);
        this.deserializer = new TDeserializer(protocolFactory);
        this.studentEvent = studentEvent;
        this.timestamp = timestamp;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        timestamp = in.readLong();
        int dataLength = in.readInt();
        studentEvent = new StudentEvent();
        byte[] data = new byte[dataLength];
        in.readFully(data);
        try {
            deserializer.deserialize(studentEvent, data);
        } catch (TException e) {
            throw new IOException("Could not deserialize StudentEvent. Got: " +
                    e.getMessage());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void write(DataOutput out) throws IOException {
        byte[] data;
        try {
            data = serializer.serialize(studentEvent);
        } catch (TException e) {
            throw new IOException("Could not serialize StudentEvent. Got: " +
                    e.getMessage());
        }
        out.writeLong(timestamp);
        out.writeInt(data.length);
        out.write(data);
    }

    public StudentEvent getStudentEvent() {
        return studentEvent;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public StudentEventWritable clone() {
        StudentEventWritable sew = new StudentEventWritable(
                new StudentEvent(studentEvent), this.timestamp);
        return sew;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        StringBuffer strBuffer = new StringBuffer();
        strBuffer.append("{ timestamp:")
                .append(timestamp)
                .append(", studentEvent: ")
                .append(studentEvent.toString())
                .append(" }");
        return strBuffer.toString();
    }

    /**
     * Comparator for StudentEventWritables based on the timestamp.
     *
     */
    public static class StudentEventTimestampComparator implements
            Comparator<StudentEventWritable> {

        @Override
        public int compare(StudentEventWritable sew1,
                StudentEventWritable sew2) {
            // Use LongComparator. Takes care of overflows.
            return new Long(sew1.timestamp).compareTo(sew2.timestamp);
        }
    }

    /**
     * Comparator for StudentEventWritables based on the student event id.
     *
     */
    public static class StudentEventIdComparator implements
            Comparator<StudentEventWritable> {

        @Override
        public int compare(StudentEventWritable sew1,
                StudentEventWritable sew2) {
            return new Long(sew1.studentEvent.getId()).
                    compareTo(sew2.studentEvent.getId());
        }
    }

}
