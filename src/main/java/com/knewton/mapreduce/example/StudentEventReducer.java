package com.knewton.mapreduce.example;

import com.knewton.thrift.StudentEvent;
import com.knewton.thrift.StudentEventData;
import com.knewton.mapreduce.io.StudentEventWritable;

import com.google.common.collect.Lists;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.*;
import java.util.Collections;
import java.util.List;

/**
 * Copyright (c) 2013 Knewton
 * 
 * Dual licensed under: MIT: http://www.opensource.org/licenses/mit-license.php GPLv3:
 * http://www.opensource.org/licenses/gpl-3.0.html.
 * 
 */
public class StudentEventReducer extends Reducer<LongWritable,
        StudentEventWritable, LongWritable, Text> {

    public static final String STUDENT_INTERACTIONS_COUNT =
            "STUDENT_INTERACTIONS";
    protected static char DELIMITER;

    /**
     * {@inheritDoc}
     */
    @Override
    public void setup(Context context) {
        Configuration conf = context.getConfiguration();
        conf.set("mapred.textoutputformat.separator",
                conf.get("com.knewton.mapred.textoutputformat.separator", ","));
        DELIMITER = context.getConfiguration().get(
                "com.knewton.output.field.delimiter", ",").charAt(0);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void reduce(LongWritable key, Iterable<StudentEventWritable> values,
            Context context) throws IOException, InterruptedException {

        List<StudentEventWritable> studentEvents = Lists.newArrayList();
        for (StudentEventWritable studentEvent : values) {
            studentEvents.add(studentEvent.clone());
        }
        Collections.sort(studentEvents,
                new StudentEventWritable.StudentEventIdComparator());

        long currentEventId = Long.MIN_VALUE;
        // Loop for keeping only the latest student events.
        for (StudentEventWritable studentEvent : studentEvents) {
            if (currentEventId == studentEvent.getStudentEvent().getId()) {
                continue;
            }
            String value = getCSVStyleOut(key, studentEvent, context);
            context.write(key, new Text(value));
            currentEventId = studentEvent.getStudentEvent().getId();
        }
    }

    /**
     * Helper method for writing the student event in CSV style.
     * 
     * @param key
     * @param studentEventWritable
     * @param context
     * @return
     */
    protected String getCSVStyleOut(LongWritable key,
            StudentEventWritable studentEventWritable, Context context) {
        StudentEvent studentEvent = studentEventWritable.getStudentEvent();
        StudentEventData eventData = studentEvent.getData();
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append(studentEvent.getId()).append(DELIMITER)
                .append(eventData.getStudentId()).append(DELIMITER)
                .append(eventData.getType()).append(DELIMITER)
                .append(eventData.getBook()).append(DELIMITER)
                .append(eventData.getCourse()).append(DELIMITER)
                .append(eventData.getScore()).append(DELIMITER)
                .append(eventData.getTimestamp());
        return strBuilder.toString();
    }

}
