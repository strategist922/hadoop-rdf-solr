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

package com.talis.hadoop.rdf;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talis.hadoop.rdf.collation.QuadsCollater;
import com.talis.hadoop.rdf.merge.IndexMerge;
import com.talis.hadoop.rdf.merge.IndexMergeReducer;
import com.talis.hadoop.rdf.solr.QuadsIndexer;

public class RdfSolrJob extends Configured implements Tool{
	
	private static final Logger LOG = LoggerFactory.getLogger(RdfSolrJob.class);

	public static final String INTERMEDIATE_QUADS_URI = "tmp/collated/";
	public static final String INTERMEDIATE_SHARDS_URI = "tmp/shards/";
	public static final String SHARDS_MANIFEST = "tmp/shard.manifest";
	
    @Override
	public int run(String[] args) throws Exception {
	  
	  if ( args.length != 4 ) {
			System.err.printf("Usage: %s [generic options] <input> <output> <solr config location> <optimize>\n", getClass().getName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
	  }
		
	  String input = args[0];
	  String output = args[1];
	  String solrConfig = args[2];
	  String optimizeIndexes = args[3];
	  
	  Configuration configuration = getConf();
	  configuration.setBoolean(IndexMergeReducer.OPTIMIZE_OUTPUT, Boolean.getBoolean(optimizeIndexes));

	  Tool collationStep = new QuadsCollater(configuration);
	  collationStep.run(new String[] { input, INTERMEDIATE_QUADS_URI });
      
	  Tool indexingStep = new QuadsIndexer(configuration);
      indexingStep.run(new String[] { INTERMEDIATE_QUADS_URI, INTERMEDIATE_SHARDS_URI, solrConfig });
      
      writeShardManifest(SHARDS_MANIFEST, INTERMEDIATE_SHARDS_URI, configuration);
      
      Tool mergeStep = new IndexMerge(configuration);
      mergeStep.run(new String[] { SHARDS_MANIFEST, output });

	  return 0;
	}

    private void  writeShardManifest(String manifestLocation, String shardLocation, Configuration configuration) throws IOException{
    	Path shardsPath = new Path(INTERMEDIATE_SHARDS_URI);
    	FileSystem fs = FileSystem.get(shardsPath.toUri(), configuration);
    	StringBuffer buf = new StringBuffer();
    	for ( FileStatus status : fs.listStatus(shardsPath)){
    		LOG.info(status.getPath() + " : " + status.isDir());
    		if(status.getPath().getName().startsWith("part-") && status.isDir()){
    			buf.append(status.getPath());
    			buf.append("\n");
    		}
    	}
    	FSDataOutputStream out = fs.create(new Path(manifestLocation));
    	out.write(buf.toString().getBytes());
    	out.flush();
    	out.close();
    	LOG.info("Shard manifest built");
    }
    
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new RdfSolrJob(), args);
		System.exit(exitCode);
	}
}