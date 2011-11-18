package com.talis.hadoop.rdf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talis.hadoop.rdf.collation.QuadsCollater;
import com.talis.hadoop.rdf.solr.DummyJob;
import com.talis.hadoop.rdf.solr.QuadsIndexer;

public class RdfSolrJob extends Configured implements Tool{
	
	private static final Logger LOG = LoggerFactory.getLogger(RdfSolrJob.class);

	public static final String INTERMEDIATE_DATA_URI = "/tmp/collated/";
	
    @Override
	public int run(String[] args) throws Exception {
	  
	  if ( args.length != 2 ) {
			System.err.printf("Usage: %s [generic options] <input> <output>\n", getClass().getName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
	  }
		
	  Configuration configuration = getConf();

	  Tool collationStep = new QuadsCollater(configuration);
	  collationStep.run(new String[] { args[0], INTERMEDIATE_DATA_URI });
	  
      Tool indexingStep = new QuadsIndexer(configuration);
      indexingStep.run(new String[] { INTERMEDIATE_DATA_URI, args[1] });
//      Tool dummyStep = new DummyJob(configuration);
//      dummyStep.run(new String[] { INTERMEDIATE_DATA_URI, args[1] });
      
	  return 0;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new RdfSolrJob(), args);
		System.exit(exitCode);
	}
}