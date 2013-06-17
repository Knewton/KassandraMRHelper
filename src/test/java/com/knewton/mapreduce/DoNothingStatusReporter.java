package com.knewton.mapreduce;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.StatusReporter;

public class DoNothingStatusReporter extends StatusReporter {

    @Override
    public Counter getCounter(Enum<?> arg0) {
        return null;
    }

    @Override
    public Counter getCounter(String arg0, String arg1) {
        return new Counter() {
        };
    }

    @Override
    public void progress() {
    }

    @Override
    public void setStatus(String arg0) {
    }

}
