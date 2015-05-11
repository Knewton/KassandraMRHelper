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

import com.knewton.mapreduce.cassandra.WriteSampleSSTable;
import com.knewton.thrift.StudentEvent;
import com.knewton.thrift.StudentEventData;

import com.google.common.collect.Lists;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.service.StorageService;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.math.RandomUtils;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.joda.time.DateTime;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Various random utility methods for generating ids and student events. Used by the sample table
 * writer.
 *
 */
public class RandomStudentEventGenerator {

    private static TProtocolFactory factory = new TCompactProtocol.Factory();
    private static TSerializer encoder = new TSerializer(factory);
    private static long initTimeInMillis = System.currentTimeMillis();

    /**
     * Generates a random student event. Sets the date of the event to now.
     *
     * @return Random student event with event time set to now.
     */
    public static StudentEvent getRandomStudentEvent() {
        StudentEventData eventData = new StudentEventData(getRandomId(),
                                                          DateTime.now().getMillis(),
                                                          RandomStringUtils.randomAlphabetic(10),
                                                          RandomUtils.nextInt(101));
        eventData.setBook(RandomStringUtils.randomAlphabetic(10));
        eventData.setCourse(RandomStringUtils.randomAlphabetic(10));
        return new StudentEvent(initTimeInMillis++, eventData);
    }

    /**
     * Generates a random long used in ids.
     *
     * @return A random ID
     */
    public static long getRandomId() {
        return RandomUtils.nextLong() + 1;
    }

    /**
     * Helper method for wrapping an id in a <code>ByteBuffer</code>.
     *
     * @return A byte buffer with a random ID
     */
    public static ByteBuffer getRandomIdBuffer() {
        long id = getRandomId();
        ByteBuffer bb = ByteBuffer.wrap(new byte[8]);
        bb.putLong(id);
        bb.rewind();
        return bb;
    }

    /**
     * Helper method for serializing a {@link StudentEventData} object.
     *
     * @param eventData
     * @return A <code>thrift</code> serialized <code>byte[]</code> using the
     *         {@link TCompactProtocol}
     * @throws TException
     */
    public static byte[] serializeStudentEventData(StudentEventData eventData) throws TException {
        byte[] data = encoder.serialize(eventData);
        return data;
    }

    /**
     * Helper method for generating decorated increasing student ids. Used by
     * {@link WriteSampleSSTable} for writing a sorted SSTable.
     *
     * @return A list of byte buffers with student IDs
     */
    public static List<ByteBuffer> getStudentIds(int numberOfStudents) {
        long studentId = 1000000L;
        List<ByteBuffer> studentIds = Lists.newArrayListWithCapacity(numberOfStudents);

        for (int i = 0; i < numberOfStudents; i++) {
            ByteBuffer bb = ByteBuffer.wrap(new byte[8]);
            bb.putLong(studentId++);
            bb.rewind();
            studentIds.add(bb);
        }

        Collections.sort(studentIds, new Comparator<ByteBuffer>() {
            @Override
            public int compare(ByteBuffer o1, ByteBuffer o2) {
                DecoratedKey dk1 = StorageService.getPartitioner().decorateKey(o1);
                DecoratedKey dk2 = StorageService.getPartitioner().decorateKey(o2);
                return dk1.compareTo(dk2);
            }
        });

        return studentIds;
    }

}
