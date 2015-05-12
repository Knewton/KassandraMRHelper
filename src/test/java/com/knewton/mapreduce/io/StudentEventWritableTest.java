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
package com.knewton.mapreduce.io;

import com.knewton.mapreduce.io.StudentEventWritable.StudentEventIdComparator;
import com.knewton.mapreduce.io.StudentEventWritable.StudentEventTimestampComparator;
import com.knewton.thrift.StudentEvent;
import com.knewton.thrift.StudentEventData;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StudentEventWritableTest {

    @Test
    public void testWriteRead() throws Exception {

        Random rand = new Random();
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(byteArrayOutputStream);

        StudentEventData seData = new StudentEventData(123L, System.currentTimeMillis(), "type", 2);
        seData.setBook("book");
        seData.setCourse("course");
        StudentEvent seEvent = new StudentEvent(rand.nextLong(), seData);
        StudentEventWritable seWritable = new StudentEventWritable(seEvent, rand.nextLong());

        seWritable.write(dout);

        StudentEventWritable deserializedWritable = new StudentEventWritable();
        InputStream is = new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
        DataInputStream dis = new DataInputStream(is);
        deserializedWritable.readFields(dis);

        assertEquals(seWritable.getTimestamp(), deserializedWritable.getTimestamp());
        assertEquals(seWritable.getStudentEvent(), deserializedWritable.getStudentEvent());
    }

    @Test(expected = IOException.class)
    public void testWriteWithException() throws Exception {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(byteArrayOutputStream);
        StudentEventWritable seWritable = new StudentEventWritable(new StudentEvent(), 0L);
        seWritable.write(dout);
    }

    @Test(expected = IOException.class)
    public void testReadWithException() throws Exception {
        ByteBuffer bb = ByteBuffer.wrap(new byte[100]);
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bb.array()));
        StudentEventWritable seWritable = new StudentEventWritable();
        seWritable.readFields(dis);
    }

    @Test
    public void testClone() {
        StudentEventData seData = new StudentEventData(123L, System.currentTimeMillis(), "type", 2);
        seData.setBook("book");
        seData.setCourse("course");
        StudentEvent seEvent = new StudentEvent(10L, seData);

        StudentEventWritable seWritable = new StudentEventWritable(seEvent, 100L);
        StudentEventWritable clonedWritable = seWritable.clone();

        assertEquals(seWritable.getStudentEvent(), clonedWritable.getStudentEvent());
        assertEquals(seWritable.getTimestamp(), clonedWritable.getTimestamp());
    }

    @Test
    public void testCompareTimestamp() {
        StudentEventTimestampComparator comparator = new StudentEventTimestampComparator();
        StudentEventWritable smaller = new StudentEventWritable(new StudentEvent(), 1L);
        StudentEventWritable bigger = new StudentEventWritable(new StudentEvent(), 2L);
        assertTrue(comparator.compare(smaller, bigger) < 0);
        assertTrue(comparator.compare(bigger, smaller) > 0);
        assertEquals(0, comparator.compare(smaller, smaller));
    }

    @Test
    public void testCompareStudentEventId() {
        StudentEventIdComparator comparator = new StudentEventIdComparator();
        StudentEvent seSmaller = new StudentEvent(1L, new StudentEventData());
        StudentEventWritable smaller = new StudentEventWritable(seSmaller, 2L);

        StudentEvent seBigger = new StudentEvent(2L, new StudentEventData());
        StudentEventWritable bigger = new StudentEventWritable(seBigger, 2L);

        assertTrue(comparator.compare(smaller, bigger) < 0);
        assertTrue(comparator.compare(bigger, smaller) > 0);
        assertEquals(0, comparator.compare(smaller, smaller));
    }
}
