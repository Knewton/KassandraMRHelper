/**
 * Copyright (c) 2013 Knewton
 * 
 * Dual licensed under: MIT: http://www.opensource.org/licenses/mit-license.php GPLv3:
 * http://www.opensource.org/licenses/gpl-3.0.html.
 * 
 */
package com.knewton.mapreduce;

import com.knewton.thrift.StudentEvent;
import com.knewton.thrift.StudentEventData;
import com.knewton.mapreduce.util.CounterConstants;

import org.apache.cassandra.db.IColumn;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

import javax.annotation.Nullable;

/**
 * Abstract mapper that converts a cassandra column to a student event and a cassandra row key to a
 * student id.
 * 
 * @param <K>
 * @param <V>
 */
@SuppressWarnings("rawtypes")
public abstract class StudentEventAbstractMapper<K extends WritableComparable, V extends Writable>
        extends SSTableColumnMapper<Long, StudentEvent, K, V> {

    private TDeserializer decoder;
    private Interval timeRange;
    private static final Logger LOG =
            LoggerFactory.getLogger(StudentEventAbstractMapper.class);

    public static final String DATE_TIME_STRING_FORMAT =
            "yyyy'-'MM'-'dd'T'HH':'mm':'ss.SSSZ"; // 2013-03-31T04:03:02.394-04:00
    public static final String START_DATE_PARAMETER_NAME =
            "com.knewton.studentevents.date.start";
    public static final String END_DATE_PARAMETER_NAME =
            "com.knewton.studentevents.date.end";
    public static final String IGNORE_TEST_UUIDS_PARAMETER_NAME =
            "com.knewton.studentevents.ignore_test_uuids";
    /**
     * Helper constant for avoiding date time overflows.
     */
    public static final long ONE_DAY_IN_MILLIS = 86400000L;

    public StudentEventAbstractMapper() {
        TProtocolFactory factory = new TCompactProtocol.Factory();
        decoder = new TDeserializer(factory);
        setSkipDeletedColumns(true);
    }

    /**
     * Setup time range for excluding student events.
     */
    @Override
    protected void setup(Context context) throws IOException,
            InterruptedException {
        super.setup(context);
        setupTimeRange(context.getConfiguration());
    }

    /**
     * @param key
     * @param context
     * @return Get the student id from the row key.
     */
    @Override
    protected Long getMapperKey(ByteBuffer key, Context context) {
        ByteBuffer dup = key.slice();
        Long studentId = dup.getLong();
        return studentId;
    }

    /**
     * @param iColumn
     * @param context
     * @return Get a student event out of a cassandra column.
     */
    @Override
    protected StudentEvent getMapperValue(IColumn iColumn, Context context) {
        return getStudentEvent(iColumn, context);
    }

    /**
     * Make a student event out of a column in cassandra.
     * 
     * @param value
     * @param context
     * @return
     */
    @Nullable
    private StudentEvent getStudentEvent(IColumn value, Context context) {
        context.getCounter(CounterConstants.STUDENT_EVENTS_JOB,
                CounterConstants.STUDENT_EVENTS_COUNT).increment(1);
        ByteBuffer columnNameDup = value.name().slice();
        ByteBuffer columnValueDup = value.value().slice();
        long eventId = columnNameDup.getLong();
        if (!isInTimeRange(eventId, context)) {
            return null;
        }
        byte[] data = new byte[columnValueDup.remaining()];
        columnValueDup.get(data);
        StudentEvent studentEvent = new StudentEvent();
        StudentEventData studentEventData = new StudentEventData();
        try {
            decoder.deserialize(studentEventData, data);
        } catch (TException e) {
            LOG.error(String.format(
                    "Could not deserialize byte array for event with id %d. Skipping Got: %s",
                    eventId, e.getMessage()));
            return null;
        }
        studentEvent.setId(eventId);
        studentEvent.setData(studentEventData);
        context.getCounter(CounterConstants.STUDENT_EVENTS_JOB,
                CounterConstants.STUDENT_EVENTS_PROCESSED).increment(1);
        return studentEvent;
    }

    /**
     * Checks to see if the event is in the desired time range.
     * 
     * @param eventId
     * @param context
     * @return
     */
    private boolean isInTimeRange(long eventId, Context context) {
        DateTime eventTime = new DateTime(eventId).withZone(DateTimeZone.UTC);
        // Skip events outside of the desired time range.
        if (timeRange != null && !timeRange.contains(eventTime)) {
            context.getCounter(CounterConstants.STUDENT_EVENTS_JOB,
                    CounterConstants.STUDENT_EVENTS_SKIPPED).increment(1);
            return false;
        }
        return true;
    }

    /**
     * Sets up a DateTime interval for excluding student events. When start time is not set then it
     * defaults to the beginning of time. If end date is not specified then it defaults to
     * "the end of time".
     * 
     * @param conf
     */
    private void setupTimeRange(Configuration conf) {
        DateTimeFormatter dtf =
                DateTimeFormat.forPattern(DATE_TIME_STRING_FORMAT).withZoneUTC();
        String startDateStr = conf.get(START_DATE_PARAMETER_NAME);
        String endDateStr = conf.get(END_DATE_PARAMETER_NAME);
        // No need to instantiate timeRange.
        if (startDateStr == null && endDateStr == null) {
            return;
        }
        DateTime startDate;
        if (startDateStr != null) {
            startDate = dtf.parseDateTime(startDateStr);
        } else {
            startDate = new DateTime(Long.MIN_VALUE + ONE_DAY_IN_MILLIS)
                    .withZone(DateTimeZone.UTC);
        }
        DateTime endDate;
        if (endDateStr != null) {
            endDate = dtf.parseDateTime(endDateStr);
        } else {
            endDate = new DateTime(Long.MAX_VALUE - ONE_DAY_IN_MILLIS)
                    .withZone(DateTimeZone.UTC);
        }
        this.timeRange = new Interval(startDate, endDate);
    }

}
