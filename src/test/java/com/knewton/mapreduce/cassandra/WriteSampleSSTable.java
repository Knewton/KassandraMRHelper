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
package com.knewton.mapreduce.cassandra;

import com.knewton.mapreduce.util.RandomStudentEventGenerator;
import com.knewton.thrift.StudentEvent;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.io.sstable.SSTableSimpleWriter;
import org.apache.cassandra.service.StorageService;
import org.apache.commons.cli.*;
import org.apache.thrift.TException;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class WriteSampleSSTable {

    private static File tableDirectory;
    private static int numberOfStudents;
    private static int eventsPerStudent;

    private static final int DEFAULT_NUM_STUDENTS = 100;
    private static final int DEFAULT_NUM_EVENTS_PER_STUDENT = 10;

    /**
     * @param args
     * @throws IOException
     * @throws ParseException
     * @throws TException
     */
    public static void main(String[] args) throws IOException, ParseException,
            TException {
        buildParametersFromArgs(args);
        CFMetaData cfMeta = new CFMetaData(
                "demoKeyspace",
                "StudentEvents",
                ColumnFamilyType.Standard,
                LongType.instance,
                null);
        SSTableSimpleWriter simpleWriter = new SSTableSimpleWriter(
                tableDirectory, cfMeta, StorageService.getPartitioner());

        List<ByteBuffer> studentIds =
                RandomStudentEventGenerator.getStudentIds(numberOfStudents);

        for (int i = 0; i < numberOfStudents; i++) {
            simpleWriter.newRow(studentIds.get(i));
            for (int j = 0; j < eventsPerStudent; j++) {
                StudentEvent studentEvent =
                        RandomStudentEventGenerator.getRandomStudentEvent();
                ByteBuffer columnName = ByteBuffer.wrap(new byte[8]);
                columnName.putLong(studentEvent.getId());
                columnName.rewind();
                ByteBuffer columnValue = ByteBuffer.wrap(
                        RandomStudentEventGenerator.serializeStudentEventData(
                                studentEvent.getData()));
                simpleWriter.addColumn(
                        columnName,
                        columnValue,
                        System.currentTimeMillis());
            }
        }

        simpleWriter.close();
    }

    /**
     * Called to validate the parameters.
     * 
     * @param args
     * @throws ParseException
     */
    private static void buildParametersFromArgs(String[] args) throws ParseException {
        CommandLineParser cliParser = new BasicParser();
        Options options = buildOptions();
        CommandLine cli = cliParser.parse(options, args);
        if (cli.getArgs().length < 1 || cli.hasOption('h')) {
            printUsage(options);
        }
        tableDirectory = new File(cli.getArgs()[0]);
        numberOfStudents = Integer.parseInt(cli.getOptionValue('s',
                String.valueOf(DEFAULT_NUM_STUDENTS)));
        eventsPerStudent = Integer.parseInt(cli.getOptionValue('e',
                String.valueOf(DEFAULT_NUM_EVENTS_PER_STUDENT)));

    }

    /**
     * Setup the options used by the random student event generator.
     * 
     * @return
     */
    private static Options buildOptions() {
        Options options = new Options();
        Option option = new Option(
                "s",
                "students",
                true,
                "The number of students (rows) to be generated. Default value is 100.");
        option.setRequired(false);
        options.addOption(option);
        option = new Option(
                "e",
                "studentEvents",
                true,
                "The number of student events per student to be generated. Default value is 10");
        option.setRequired(false);
        options.addOption(option);
        option = new Option(
                "h",
                "help",
                false,
                "Prints this help message.");
        option.setRequired(false);
        options.addOption(option);
        return options;
    }

    /**
     * Prints usage information for running this program with all the options.
     * 
     * @param options
     */
    private static void printUsage(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp(WriteSampleSSTable.class.getSimpleName() +
                " [OPTIONS] <output_dir>", options);
        System.exit(0);
    }

}
