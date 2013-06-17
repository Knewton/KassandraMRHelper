/**
 * Copyright (c) 2013 Knewton
 * 
 * Dual licensed under: MIT: http://www.opensource.org/licenses/mit-license.php GPLv3:
 * http://www.opensource.org/licenses/gpl-3.0.html.
 * 
 */
package com.knewton.mapreduce.example;

import com.knewton.mapreduce.io.SSTableColumnInputFormat;
import com.knewton.mapreduce.io.SSTableInputFormat;
import com.knewton.mapreduce.io.StudentEventWritable;

import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;

/**
 * Example job for reading data from SSTables. This example uses the StudentEvents column family
 * that stores serialized StudentEvent objects defined in a thrift specification under the thrift
 * source directory. Sample SSTables can be generated with {@link WriteSampleSSTable}.
 * 
 * @author Giannis Neokleous
 * 
 */
public class SSTableMRExample {

    private static final Logger LOG = LoggerFactory.getLogger(
            SSTableMRExample.class);

    /**
     * @param args
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     * @throws URISyntaxException
     * @throws ParseException
     */
    public static void main(String[] args) throws IOException,
            InterruptedException, ClassNotFoundException, URISyntaxException,
            ParseException {

        long startTime = System.currentTimeMillis();
        Options options = buildOptions();

        CommandLineParser cliParser = new BasicParser();
        CommandLine cli = cliParser.parse(options, args);
        if (cli.getArgs().length < 2 || cli.hasOption('h')) {
            printUsage(options);
        }
        Job job = getJobConf(cli);

        job.setJarByClass(SSTableMRExample.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(StudentEventWritable.class);

        job.setMapperClass(StudentEventMapper.class);
        job.setReducerClass(StudentEventReducer.class);

        job.setInputFormatClass(SSTableColumnInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        // input arg
        String inputPaths = cli.getArgs()[0];
        LOG.info("Setting initial input paths to {}", inputPaths);
        SSTableInputFormat.addInputPaths(job, inputPaths);
        // output arg
        FileOutputFormat.setOutputPath(job, new Path(cli.getArgs()[1]));
        if (cli.hasOption('c')) {
            LOG.info("Using compression for output.");
            FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
            FileOutputFormat.setCompressOutput(job, true);
        }
        job.waitForCompletion(true);
        LOG.info("Total runtime: {}s",
                (System.currentTimeMillis() - startTime) / 1000);
    }

    private static Job getJobConf(CommandLine cli)
            throws URISyntaxException, IOException {
        Configuration conf = new Configuration();
        Job job = new Job(conf);
        ClassLoader loader = SSTableMRExample.class.getClassLoader();
        URL url = loader.getResource("knewton-site.xml");
        conf.addResource(url);

        SSTableInputFormat.setPartitionerClass(
                RandomPartitioner.class.getName(), job);
        SSTableInputFormat.setComparatorClass(LongType.class.getName(), job);
        SSTableInputFormat.setColumnFamilyName("StudentEvents", job);

        if (cli.hasOption('s')) {
            conf.set(StudentEventMapper.START_DATE_PARAMETER_NAME,
                    cli.getOptionValue('s'));
        }
        if (cli.hasOption('e')) {
            conf.set(StudentEventMapper.END_DATE_PARAMETER_NAME,
                    cli.getOptionValue('e'));
        }
        return job;
    }

    /**
     * Prints usage information for running this program with all the options.
     * 
     * @param options
     */
    private static void printUsage(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp(
                "SSTableMRExample [OPTIONS] <input_dir> <output_dir>", options);
        System.exit(0);
    }

    private static Options buildOptions() {
        Options options = new Options();
        Option option = new Option(
                "s",
                "startDate",
                true,
                "The start date that student events should get included from. If not specified then it defaults to the beginning of time.");
        option.setRequired(false);
        options.addOption(option);
        option = new Option(
                "e",
                "endDate",
                true,
                "The end date that student events should get included. If not specified then it defaults to the \"end of time\".");
        option.setRequired(false);
        options.addOption(option);
        option = new Option(
                "c",
                "compress",
                false,
                "Set this option if you want the output to be compressed.");
        option.setRequired(false);
        options.addOption(option);
        options.addOption("h", "help", false, "Prints this help message.");
        return options;
    }

}
