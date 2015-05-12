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

import com.knewton.mapreduce.SSTableRecordReader;
import com.knewton.mapreduce.SSTableRowRecordReader;

import org.apache.cassandra.io.sstable.SSTableIdentityIterator;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class SSTableRowInputFormatTest {

    @Test
    public void testCreateRecordReader() throws Exception {
        InputSplit mockInputSplit = mock(InputSplit.class);
        TaskAttemptContext mockContext = mock(TaskAttemptContext.class);
        SSTableRowInputFormat inputFormat = new SSTableRowInputFormat();
        SSTableRecordReader<ByteBuffer, SSTableIdentityIterator> rr =
            inputFormat.createRecordReader(mockInputSplit, mockContext);
        assertNotNull(rr);
        assertTrue(rr instanceof SSTableRowRecordReader);
    }

}
