package com.knewton.mapreduce;

import com.knewton.thrift.StudentEvent;
import com.knewton.mapreduce.io.StudentEventWritable;

import org.apache.hadoop.io.LongWritable;

import java.io.IOException;

public class DoNothingStudentEventMapper
        extends StudentEventAbstractMapper<LongWritable, StudentEventWritable> {

    /**
     * {@inheritDoc}
     */
    @Override
    public void performMapTask(Long key, StudentEvent value, Context context)
            throws IOException, InterruptedException {
    }

}
