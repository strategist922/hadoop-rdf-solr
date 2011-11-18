/*
 *    Copyright 2011 Talis Systems Ltd
 * 
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 * 
 *        http://www.apache.org/licenses/LICENSE-2.0
 * 
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.talis.hadoop.rdf.merge;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.jena.tdbloader3.Constants;
import org.apache.jena.tdbloader3.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexMerge extends Configured implements Tool {

	private static final Logger LOG = LoggerFactory.getLogger(IndexMerge.class);

	public static final String JOB_NAME = "IndexMerge";

	public IndexMerge() {
		super();
		LOG.debug("Constructed with no configuration.");
	}

	public IndexMerge(Configuration configuration) {
		super(configuration);
		LOG.debug("Constructed with configuration.");
	}

	public int run(String[] args) throws Exception {

		Configuration configuration = getConf();
        
		boolean useCompression = configuration.getBoolean(Constants.OPTION_USE_COMPRESSION, Constants.OPTION_USE_COMPRESSION_DEFAULT);
		if ( useCompression ) {
            configuration.setBoolean("mapred.compress.map.output", true);
    	    configuration.set("mapred.output.compression.type", "BLOCK");
    	    configuration.set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        }

        boolean overrideOutput = configuration.getBoolean(Constants.OPTION_OVERRIDE_OUTPUT, Constants.OPTION_OVERRIDE_OUTPUT_DEFAULT);
        FileSystem fs = FileSystem.get(new Path(args[1]).toUri(), configuration);
        if ( overrideOutput ) {
            fs.delete(new Path(args[1]), true);
        }
        
        Job job = new Job(configuration);
		job.setJobName(JOB_NAME);
		job.setJarByClass(getClass());

		Path input = new Path(args[0]);
		Path output = new Path(args[1]);
		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);
		
		job.setMapperClass(Mapper.class);
		job.setReducerClass(IndexMergeReducer.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setNumReduceTasks(1);
		
		if ( LOG.isDebugEnabled() ) Utils.log(job, LOG);

		return job.waitForCompletion(true) ? 0 : -1;
	}

	@SuppressWarnings("PMD.SystemPrintln")
	public static void main(String[] args) throws Exception {
		LOG.debug("main method: {}", Utils.toString(args));
		int exitCode = ToolRunner.run(new Configuration(), new IndexMerge(), args);
		System.exit(exitCode);
	}

}
