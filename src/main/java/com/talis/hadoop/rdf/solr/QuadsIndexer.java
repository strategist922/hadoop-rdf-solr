package com.talis.hadoop.rdf.solr;


import java.io.File;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.jena.tdbloader3.Constants;
import org.apache.jena.tdbloader3.Utils;
import org.apache.solr.hadoop.SolrDocumentConverter;
import org.apache.solr.hadoop.SolrOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
		FileSystem fs = FileSystem.get(new Path(args[1]).toUri(), configuration);
		if ( overrideOutput ) {
			fs.delete(new Path(args[1]), true);
		}

		Job job = new Job(configuration);
		job.setJobName(JOB_NAME);
		job.setJarByClass(getClass());

		int shards = -1;
		boolean compressOutput = false;
	
		Path input = new Path(args[0]);
		Path output = new Path(args[1]);
		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);

//		for (int i = 1; i < args.length; i++) {
//			if (args[i] == null) continue;
//			if (args[i].equals("-shards")) {
//				shards = Integer.parseInt(args[++i]);
//			} else if (useCompression) {
//				compressOutput = true;
//			} else if (args[i].equals("-solr")) {
//				solrHome = args[++i];
//				continue;
//			} else {
//				Path p = new Path(args[i]);
//				FileInputFormat.addInputPath(job, p);
//			}
//		}
//		if (solrHome == null || !new File(solrHome).exists()) {
//			throw new IOException("You must specify a valid solr.home directory!");
//		}
		if (shards > 0) {
			job.setNumReduceTasks(shards);
		}
		
		job.setMapperClass(SolrMapper.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);

		job.setReducerClass(SolrReducer.class);
		SolrDocumentConverter.setSolrDocumentConverter(LiteralsIndexer.class, job.getConfiguration());
				
		job.setOutputFormatClass(SolrOutputFormat.class);
//		File solrConf = new File("/tmp/sxt");
//		SolrOutputFormat.setupSolrHomeCache(solrConf, job.getConfiguration());
		File zipPath = new File("/tmp/sxt/solr.zip");
		String zipName = "solr.zip";
		
		final URI baseZipUrl = fs.getUri().resolve(zipPath.toString() + '#' + zipName);
		DistributedCache.addCacheArchive(baseZipUrl, job.getConfiguration());
//		for (Path path : DistributedCache.getLocalCacheArchives(job.getConfiguration())){
//			System.out.println(path.toString());
//		}
		
		job.getConfiguration().set(SolrOutputFormat.SETUP_OK, zipPath.toString());
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
