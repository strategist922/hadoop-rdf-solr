package com.talis.hadoop.rdf.solr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.jena.tdbloader3.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SolrShardAggregatorJob extends Configured implements Tool{

	private static final Logger LOG = LoggerFactory.getLogger(SolrShardAggregatorJob.class);
	
	public static final String JOB_NAME = "SolrAggregator";
	
	public SolrShardAggregatorJob() {
		super();
        LOG.debug("Constructed with no configuration.");
	}
	
	public SolrShardAggregatorJob(Configuration configuration) {
		super(configuration);
        LOG.debug("Constructed with configuration.");
	}
	
	@Override
	public int run(String[] arg0) throws Exception {
		Configuration configuration = getConf();
		Job job = new Job(configuration);
		job.setJobName(JOB_NAME);
		job.setJarByClass(getClass());
		
		job.setNumReduceTasks(1);
		
		// TODO Auto-generated method stub
		return 0;
	}

	
	
}
