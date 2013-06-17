/**
 * Copyright (c) 2013 Knewton
 * 
 * Dual licensed under: MIT: http://www.opensource.org/licenses/mit-license.php GPLv3:
 * http://www.opensource.org/licenses/gpl-3.0.html.
 * 
 */
package com.knewton.mapreduce.example;

import com.knewton.thrift.StudentEvent;
import com.knewton.mapreduce.StudentEventAbstractMapper;
import com.knewton.mapreduce.io.StudentEventWritable;

import org.apache.hadoop.io.LongWritable;

import java.io.IOException;

public class StudentEventMapper extends StudentEventAbstractMapper<LongWritable,
        StudentEventWritable> {

    /**
     * {@inheritDoc}
     */
    @Override
    public void performMapTask(Long key, StudentEvent value,
            Context context) throws IOException, InterruptedException {
        context.write(new LongWritable(key),
                new StudentEventWritable(value, getIColumn().timestamp()));
    }

}
