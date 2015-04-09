/**
 * Jyoti Salitra
 * UTA ID: ***********
 * Cloud Computing (CSE - 6331) - David Levine
 * Programming Assignment # 2
 * Date: 10/05/2014
 */

package edu.uta.cse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Main class that configures and then starts the mapreduce tasks
 * 
 * References:
 * 1. http://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html#Example:_WordCount_v1.0
 * 
 */
public class ClimateDataRunner {

	public static void main(String[] args) throws Exception {

		// default values
		int mapper = 1, reducer = 1;
		String jobType = "YEAR";

		// check if user has supplied jobType
		if (args.length < 3) {
			System.err.println("ClimateDataRunner Usage");
			System.err
					.println("climateData.jar edu.uta.cse.ClimateDataRunner <INPUT> <OUTPUT> <MONTH/SEASON/YEAR> [<#MAP> <#REDUCE>]");
			System.exit(1);
		} else {
			jobType = args[2].toUpperCase();
		}

		System.out.println("JOB TYPE: " + jobType);

		// check if user has supplied numMapper
		try {
			if (args.length > 4) {
				mapper = Integer.parseInt(args[3]);
			}
		} catch (NumberFormatException nfe) {
		}

		// check if user has supplied numReducer
		try {
			if (args.length == 5) {
				reducer = Integer.parseInt(args[4]);
			}
		} catch (NumberFormatException nfe) {
		}

		System.out.println("ClimateData Job will use " + mapper
				+ " map tasks and " + reducer + " reduce tasks.");

		// start timer
		long start = System.currentTimeMillis();

		// create mapreduce job configuration
		Configuration conf = new Configuration();

		// set the jobType in conf so that Map task can use it
		conf.set("jobType", jobType);

		// set number of mappers to use.
		// According to Apache Hadoop guide, this is just a hint to the
		// framework.
		// Actual number of mappers used is determined by the framework based on
		// the split
		conf.set(JobContext.NUM_MAPS, String.valueOf(mapper));

		// name the mapreduce task
		Job job = Job.getInstance(conf, "climateData");

		job.setJarByClass(ClimateDataRunner.class);

		// set output data types for key and values
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongArrayWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// set mapper and reducer classes
		job.setMapperClass(ClimateDataMapper.class);
		job.setReducerClass(ClimateDataReducer.class);

		// set number of reducers to use
		job.setNumReduceTasks(reducer);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// set input and output HDFS paths
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);

		// calculate time spent in the process
		long totalTime = System.currentTimeMillis() - start;
		System.out.println("\n================= TOTAL TIME [in ms]: "
				+ totalTime);
	}
}
