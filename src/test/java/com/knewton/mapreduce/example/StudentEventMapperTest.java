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
package com.knewton.mapreduce.example;

import com.knewton.mapreduce.constant.PropertyConstants;
import com.knewton.mapreduce.io.StudentEventWritable;
import com.knewton.mapreduce.util.RandomStudentEventGenerator;

import org.apache.cassandra.db.BufferCell;
import org.apache.cassandra.db.Cell;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.SimpleDenseCellNameType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.nio.ByteBuffer;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests the {@link StudentEventMapper}
 *
 * @author Giannis Neokleous
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class StudentEventMapperTest {

    private StudentEventMapper underTest;

    @Mock
    private StudentEventMapper.Context mockedContext;

    private Configuration conf;

    private SimpleDenseCellNameType simpleDenseCellType;

    @Before
    public void setUp() throws Exception {
        underTest = new StudentEventMapper();
        conf = new Configuration(false);
        Counter mockCounter = mock(Counter.class);
        when(mockedContext.getConfiguration()).thenReturn(conf);
        when(mockedContext.getCounter(anyString(), anyString())).thenReturn(mockCounter);
        this.simpleDenseCellType = new SimpleDenseCellNameType(BytesType.instance);
    }

    /**
     * Test the start time range filters in the mapper.
     */
    @Test
    public void testMapWithStartRangeStudentEvents() throws Exception {
        conf.set(PropertyConstants.START_DATE.txt, "2013-03-28T23:03:02.394Z");
        underTest.setup(mockedContext);

        // send event outside of time range

        // Mar.28.2013.19:53:38.0.0
        DateTime dt = new DateTime(2013, 3, 28, 19, 53, 10, DateTimeZone.UTC);
        long eventId = dt.getMillis();

        ByteBuffer randomKey = RandomStudentEventGenerator.getRandomIdBuffer();
        ByteBuffer columnName = ByteBuffer.wrap(new byte[8]);
        columnName.putLong(eventId);
        columnName.rewind();
        CellName cellName = simpleDenseCellType.cellFromByteBuffer(columnName);
        Cell column = new BufferCell(cellName,
                                     ByteBuffer.wrap(Hex.decodeHex(eventDataString.toCharArray())));

        underTest.map(randomKey, column, mockedContext);
        verify(mockedContext, never()).write(any(LongWritable.class),
                                             any(StudentEventWritable.class));

        // send event inside of time range

        // Mar.29.2013.03:07:21.0.0
        dt = new DateTime(2013, 3, 29, 3, 7, 21, DateTimeZone.UTC);
        eventId = dt.getMillis();

        columnName = ByteBuffer.wrap(new byte[8]);
        columnName.putLong(eventId);
        columnName.rewind();
        cellName = simpleDenseCellType.cellFromByteBuffer(columnName);
        column = new BufferCell(cellName,
                                ByteBuffer.wrap(Hex.decodeHex(eventDataString.toCharArray())));
        randomKey = RandomStudentEventGenerator.getRandomIdBuffer();
        underTest.map(randomKey, column, mockedContext);
        verify(mockedContext).write(any(LongWritable.class), any(StudentEventWritable.class));
    }

    /**
     * Test the end time range filters in the mapper.
     */
    @Test
    public void testEndRangeStudentEvents() throws Exception {
        conf.set(PropertyConstants.END_DATE.txt, "2013-03-28T23:03:02.394Z");
        underTest.setup(mockedContext);

        // send event outside of time range

        // Mar.29.2013.03:07:21.0.0
        DateTime dt = new DateTime(2013, 3, 29, 3, 7, 21, DateTimeZone.UTC);
        long eventId = dt.getMillis();
        ByteBuffer randomKey = RandomStudentEventGenerator.getRandomIdBuffer();
        ByteBuffer columnName = ByteBuffer.wrap(new byte[8]);
        columnName.putLong(eventId);
        columnName.rewind();
        CellName cellName = simpleDenseCellType.cellFromByteBuffer(columnName);
        Cell column = new BufferCell(cellName,
                                     ByteBuffer.wrap(Hex.decodeHex(eventDataString.toCharArray())));
        underTest.map(randomKey, column, mockedContext);
        verify(mockedContext, never()).write(any(LongWritable.class),
                                             any(StudentEventWritable.class));

        // send event inside of time range

        // Mar.28.2013.19:53:38.0.0
        dt = new DateTime(2013, 3, 28, 19, 53, 10, DateTimeZone.UTC);
        eventId = dt.getMillis();
        randomKey = RandomStudentEventGenerator.getRandomIdBuffer();
        columnName = ByteBuffer.wrap(new byte[8]);
        columnName.putLong(eventId);
        columnName.rewind();
        cellName = simpleDenseCellType.cellFromByteBuffer(columnName);
        column = new BufferCell(cellName,
                                ByteBuffer.wrap(Hex.decodeHex(eventDataString.toCharArray())));
        underTest.map(randomKey, column, mockedContext);
        verify(mockedContext).write(any(LongWritable.class),
                                    any(StudentEventWritable.class));
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
