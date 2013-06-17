package com.knewton.mapreduce.util;

import static org.junit.Assert.*;

import org.junit.Test;

public class MapReduceEnvironmentTest {

    @Test
    public void testFromString() {
        MREnvironment mre =
                MREnvironment.fromValue("HadoopMapReduce");
        assertEquals(mre, MREnvironment.HADOOP_MAPREDUCE);
        mre = MREnvironment.fromValue("local");
        assertEquals(mre, MREnvironment.LOCAL);
        mre = MREnvironment.fromValue("EMR");
        assertEquals(mre, MREnvironment.EMR);
    }

}
