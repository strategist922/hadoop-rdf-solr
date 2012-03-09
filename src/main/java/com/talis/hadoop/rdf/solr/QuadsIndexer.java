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

package com.talis.hadoop.rdf.solr;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.jena.tdbloader4.Constants;
import org.apache.jena.tdbloader4.Utils;
import org.apache.solr.hadoop.SolrDocumentConverter;
import org.apache.solr.hadoop.SolrOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talis.hadoop.rdf.QuadArrayWritable;

public class QuadsIndexer extends Configured implements Tool {

	private static final Logger LOG = LoggerFactory.getLogger(QuadsIndexer.class);

	public static final String JOB_NAME = "QuadsIndexing";

	public QuadsIndexer() {
		super();
		LOG.debug("Constructed with no configuration.");
	}

	public QuadsIndexer(Configuration configuration) {
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
		FileSystem outputFs = FileSystem.get(new Path(args[1]).toUri(), configuration);
		if ( overrideOutput ) {
			outputFs.delete(new Path(args[1]), true);
		}

		Job job = new Job(configuration);
		job.setJobName(JOB_NAME);
		job.setJarByClass(getClass());

		int shards = -1;
		boolean compressOutput = false;
	
		Path input = new Path(args[0]);
		Path output = new Path(args[1]);
		Path solrConfig = new Path(args[2]);
		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);
		
		if (shards > 0) {
			job.setNumReduceTasks(shards);
		}
		
		job.setMapperClass(Mapper.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(QuadArrayWritable.class);

		job.setReducerClass(SolrReducer.class);
		SolrDocumentConverter.setSolrDocumentConverter(LiteralsIndexer.class, job.getConfiguration());
				
		job.setOutputFormatClass(SolrOutputFormat.class);
		
		String zipName = "solr.zip";
		FileSystem solrConfigFs = FileSystem.get(solrConfig.toUri(), configuration);
		final URI baseZipUrl = solrConfigFs.getUri().resolve(solrConfig.toString() + '#' + zipName);
		DistributedCache.addCacheArchive(baseZipUrl, job.getConfiguration());
		job.getConfiguration().set(SolrOutputFormat.SETUP_OK, solrConfig.toString());
		SolrOutputFormat.setOutputZipFormat(compressOutput, job.getConfiguration());

		if ( LOG.isDebugEnabled() ) Utils.log(job, LOG);

		return job.waitForCompletion(true) ? 0 : -1;
	}

	@SuppressWarnings("PMD.SystemPrintln")
	public static void main(String[] args) throws Exception {
		LOG.debug("main method: {}", Utils.toString(args));
		int exitCode = ToolRunner.run(new Configuration(), new QuadsIndexer(), args);
		System.exit(exitCode);
	}

}
