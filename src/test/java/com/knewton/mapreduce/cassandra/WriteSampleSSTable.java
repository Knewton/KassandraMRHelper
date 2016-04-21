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
package com.knewton.mapreduce.cassandra;

import com.knewton.mapreduce.example.SSTableMRExample;
import com.knewton.mapreduce.util.RandomStudentEventGenerator;
import com.knewton.thrift.StudentEvent;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.apache.cassandra.service.StorageService;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.File;
import java.nio.ByteBuffer;

/**
 * Use this class to generate a sample SSTable that can be used to run the example job included in
 * this jar called {@link SSTableMRExample}
 *
 * @author Giannis Neokleous
 *
 */
public class WriteSampleSSTable {

    private static File tableDirectory;
    private static int numberOfStudents;
    private static int eventsPerStudent;
    private static final String KEYSPACE_NAME = "demoKeyspace";
    private static final String COLUMN_FAMILY_NAME = "StudentEvents";

    private static final int DEFAULT_NUM_STUDENTS = 100;
    private static final int DEFAULT_NUM_EVENTS_PER_STUDENT = 10;

    /**
     * Writes a sample SSTable that can be used for running the example job {@link SSTableMRExample}
     *
     * @param args
     *            Args to be parsed
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        buildParametersFromArgs(args);

        IPartitioner partitioner = StorageService.getPartitioner();
        String schema = String.format("CREATE TABLE %s.%s (studentid 'LongType', "
                                                        + "eventid 'LongType',"
                                                        + "data 'BytesType', "
                                                        + "PRIMARY KEY (studentid, eventid))"
                         + " WITH COMPACT STORAGE", KEYSPACE_NAME, COLUMN_FAMILY_NAME);

        String insertStatement = String.format("INSERT INTO %s.%s (studentid, eventid, data) "
            + "VALUES (?, ?, ?)", KEYSPACE_NAME, COLUMN_FAMILY_NAME);

        CQLSSTableWriter tableWriter = CQLSSTableWriter.builder()
                                                        .inDirectory(tableDirectory)
                                                        .withPartitioner(partitioner)
                                                        .forTable(schema)
                                                        .using(insertStatement)
                                                        .build();

        for (int i = 0; i < numberOfStudents; i++) {
            for (int j = 0; j < eventsPerStudent; j++) {
                StudentEvent studentEvent =
                        RandomStudentEventGenerator.getRandomStudentEvent();

                ByteBuffer columnValue = ByteBuffer.wrap(
                        RandomStudentEventGenerator.serializeStudentEventData(
                                studentEvent.getData()));

                tableWriter.addRow(RandomStudentEventGenerator.getRandomId(),
                                   studentEvent.getId(),
                                   columnValue);
            }
        }

        tableWriter.close();
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
        tableDirectory = new File(cli.getArgs()[0] +
                                  String.format("/%s/%s/", KEYSPACE_NAME, COLUMN_FAMILY_NAME));
        tableDirectory.mkdirs();
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
        formatter.printHelp(String.format("%s [OPTIONS] <output_dir>",
                                          WriteSampleSSTable.class.getSimpleName()),
                            options);
        System.exit(0);
    }

}
