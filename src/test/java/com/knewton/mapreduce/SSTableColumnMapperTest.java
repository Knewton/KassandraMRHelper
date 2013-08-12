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
package com.knewton.mapreduce;

import static org.junit.Assert.*;

import com.knewton.mapreduce.io.StudentEventWritable;
import com.knewton.mapreduce.util.RandomStudentEventGenerator;

import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.DeletedColumn;
import org.apache.cassandra.db.IColumn;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.*;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

public class SSTableColumnMapperTest {

    private DoNothingColumnMapper concreteMapper;
    private String keyVal;
    private String columnNameVal;
    private String columnValue;

    @Before
    public void setUp() throws Exception {
        this.concreteMapper = new DoNothingColumnMapper();
        this.keyVal = "testKey";
        this.columnNameVal = "testColumnName";
        this.columnValue = "testColumnValue";
    }

    /**
     * Test to see if cassandra columns go through the mapper correctly.
     * 
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void testMap() throws IOException, InterruptedException {

        ByteBuffer key = ByteBuffer.wrap(keyVal.getBytes());
        IColumn column = new Column(ByteBuffer.wrap(columnNameVal.getBytes()),
                ByteBuffer.wrap(columnValue.getBytes()));
        concreteMapper.map(key, column, null);
        // Key
        byte[] verifyByteArr = new byte[keyVal.length()];
        concreteMapper.getCurrentKey().get(verifyByteArr);
        assertEquals(keyVal, new String(verifyByteArr));
        // Column name
        verifyByteArr = new byte[columnNameVal.length()];
        concreteMapper.getCurrentColumn().name().get(verifyByteArr);
        assertEquals(columnNameVal, new String(verifyByteArr));
        // Column value
        verifyByteArr = new byte[columnValue.length()];
        concreteMapper.getCurrentColumn().value().get(verifyByteArr);
        assertEquals(columnValue, new String(verifyByteArr));
        concreteMapper.clearKeyVal();
    }

    /**
     * Test the start time range filters in the mapper.
     * 
     * @throws IOException
     * @throws InterruptedException
     * @throws DecoderException
     */
    @Test
    public void testStartRangeStudentEvents() throws IOException,
            InterruptedException, DecoderException {

        // Mar.28.2013.19:53:38.0.0
        DateTime dt = new DateTime(2013, 3, 28, 19, 53, 10, DateTimeZone.UTC);
        long eventId1 = dt.getMillis();
        // Mar.29.2013.03:07:21.0.0
        dt = new DateTime(2013, 3, 29, 3, 7, 21, DateTimeZone.UTC);
        long eventId5 = dt.getMillis();

        ByteBuffer columnName = ByteBuffer.wrap(new byte[8]);
        columnName.putLong(eventId1);
        columnName.rewind();
        IColumn column = new Column(columnName,
                ByteBuffer.wrap(Hex.decodeHex(eventDataString.toCharArray())));
        Configuration conf = new Configuration();
        conf.set(StudentEventAbstractMapper.START_DATE_PARAMETER_NAME,
                "2013-03-28T23:03:02.394-04:00");
        DoNothingStudentEventMapper dnsem = new DoNothingStudentEventMapper();
        Mapper<ByteBuffer, IColumn, LongWritable, StudentEventWritable>.Context context =
                dnsem.new Context(conf, new TaskAttemptID(), null, null, null,
                        new DoNothingStatusReporter(), null);
        dnsem.setup(context);
        dnsem.map(RandomStudentEventGenerator.getRandomIdBuffer(),
                column, context);
        assertNull(dnsem.getRowKey());
        columnName = ByteBuffer.wrap(new byte[8]);
        columnName.putLong(eventId5);
        columnName.rewind();
        column = new Column(columnName,
                ByteBuffer.wrap(Hex.decodeHex(eventDataString.toCharArray())));
        ByteBuffer randomKey = RandomStudentEventGenerator.getRandomIdBuffer();
        dnsem.map(randomKey, column, context);
        assertEquals(dnsem.getRowKey(), randomKey);
    }

    /**
     * Test the end time range filters in the mapper.
     * 
     * @throws IOException
     * @throws InterruptedException
     * @throws DecoderException
     */
    @Test
    public void testEndRangeStudentEvents() throws IOException,
            InterruptedException, DecoderException {

        // Mar.28.2013.19:53:38.0.0
        DateTime dt = new DateTime(2013, 3, 28, 19, 53, 10, DateTimeZone.UTC);
        long eventId1 = dt.getMillis();
        // Mar.29.2013.03:07:21.0.0
        dt = new DateTime(2013, 3, 29, 3, 7, 21, DateTimeZone.UTC);
        long eventId5 = dt.getMillis();

        ByteBuffer columnName = ByteBuffer.wrap(new byte[8]);
        columnName.putLong(eventId5);
        columnName.rewind();
        IColumn column = new Column(columnName,
                ByteBuffer.wrap(Hex.decodeHex(eventDataString.toCharArray())));
        Configuration conf = new Configuration();
        conf.set(StudentEventAbstractMapper.END_DATE_PARAMETER_NAME,
                "2013-03-28T23:03:02.394-04:00");
        DoNothingStudentEventMapper dnsem = new DoNothingStudentEventMapper();
        Mapper<ByteBuffer, IColumn, LongWritable, StudentEventWritable>.Context context =
                dnsem.new Context(conf, new TaskAttemptID(), null, null, null,
                        new DoNothingStatusReporter(), null);
        dnsem.setup(context);
        dnsem.map(RandomStudentEventGenerator.getRandomIdBuffer(),
                column, context);
        assertNull(dnsem.getRowKey());
        ByteBuffer randomKey = RandomStudentEventGenerator.getRandomIdBuffer();
        columnName = ByteBuffer.wrap(new byte[8]);
        columnName.putLong(eventId1);
        columnName.rewind();
        column = new Column(columnName,
                ByteBuffer.wrap(Hex.decodeHex(eventDataString.toCharArray())));
        dnsem.map(randomKey, column, context);
        assertEquals(dnsem.getRowKey(), randomKey);
    }

    /**
     * Tests to see if deleted columns don't flow through the mapper.
     * 
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void testMapDeletedColumn() throws IOException,
            InterruptedException {
        concreteMapper.setSkipDeletedColumns(true);
        IColumn column = new DeletedColumn(
                ByteBuffer.wrap(columnNameVal.getBytes()),
                ByteBuffer.wrap(columnValue.getBytes()),
                System.currentTimeMillis() - 100);
        concreteMapper.map(ByteBuffer.wrap(keyVal.getBytes()), column, null);
        assertNull(concreteMapper.getCurrentKey());
        assertNull(concreteMapper.getCurrentColumn());
        concreteMapper.clearKeyVal();
    }

    /**
     * Tests the skip deleted columns option
     */
    @Test
    public void testSetSkipDeletedColumns() {
        concreteMapper.setSkipDeletedColumns(true);
        assertTrue(concreteMapper.isSkipDeletedColumns());
        concreteMapper.setSkipDeletedColumns(false);
        assertTrue(!concreteMapper.isSkipDeletedColumns());
    }

    /**
     * A hexadecimal representation of a serialized student event. Used for testing as values to the
     * cassandra columns.
     */
    private static final String eventDataString =
            "169ad7a6d4e5d2abec4d16bae2bed5e04f181ee7aca9e188a5ec8d90eabe83ec" +
                    "8594ee9bb2e6aaaeefa182e39184e7a3941508181dd480e28c98e692" +
                    "8de29a9fe7bc9bebb380ec99bfe480b0e990bde7b0a9181de29291eb" +
                    "86bfe582b3e0b686e2879cd79ce2838ae3a2aae3a9bee7949000";
}
