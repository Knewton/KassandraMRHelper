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
