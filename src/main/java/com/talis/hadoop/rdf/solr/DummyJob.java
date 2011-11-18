package com.talis.hadoop.rdf.solr;


import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.jena.tdbloader3.Constants;
import org.apache.jena.tdbloader3.Utils;
import org.apache.jena.tdbloader3.io.NQuadsInputFormat;
import org.apache.solr.hadoop.SolrDocumentConverter;
import org.apache.solr.hadoop.SolrOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DummyJob extends Configured implements Tool {

	private static final Logger LOG = LoggerFactory.getLogger(DummyJob.class);

	public static final String JOB_NAME = "DummyJob";

	public DummyJob() {
		super();
		LOG.debug("Constructed with no configuration.");
	}

	public DummyJob(Configuration configuration) {
		super(configuration);
		LOG.debug("Constructed with configuration.");
	}

	public int run(String[] args) throws Exception {

		Configuration configuration = getConf();
		
		Job job = new Job(configuration);
		job.setJobName(JOB_NAME);
		job.setJarByClass(getClass());

		Path input = new Path(args[0]);
		Path output = new Path(args[1]);
		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);
		
		job.setMapperClass(DummyMapper.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(Writable.class);
		job.setOutputValueClass(Writable.class);

		job.setReducerClass(DummyReducer.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		if ( LOG.isDebugEnabled() ) Utils.log(job, LOG);

		return job.waitForCompletion(true) ? 0 : -1;
	}

	@SuppressWarnings("PMD.SystemPrintln")
	public static void main(String[] args) throws Exception {
		LOG.debug("main method: {}", Utils.toString(args));
		int exitCode = ToolRunner.run(new Configuration(), new DummyJob(), args);
		System.exit(exitCode);
	}

}
