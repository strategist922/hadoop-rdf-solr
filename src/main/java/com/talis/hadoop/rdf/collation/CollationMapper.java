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

package com.talis.hadoop.rdf.collation;

import static com.talis.hadoop.rdf.Constants.QUADS_ACCEPTED;
import static com.talis.hadoop.rdf.Constants.QUADS_DROPPED;
import static com.talis.hadoop.rdf.Constants.QUADS_READ;
import static com.talis.hadoop.rdf.Constants.RDF_SOLR_COUNTER_GROUP;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.jena.tdbloader3.io.QuadWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.core.Quad;

public class CollationMapper extends Mapper<LongWritable, QuadWritable, Text, QuadWritable>{

	private static final Logger LOG = LoggerFactory.getLogger(CollationMapper.class);
	
	@Override
	public void map(LongWritable key, QuadWritable value, Context context) throws IOException, InterruptedException {
		Quad quad = value.getQuad();
		LOG.debug("Quad: {} ", quad.toString());
		context.getCounter(RDF_SOLR_COUNTER_GROUP, QUADS_READ).increment(1);
		Node subject = quad.getSubject();
		if ((null != subject) && (subject.isURI())){
			context.getCounter(RDF_SOLR_COUNTER_GROUP, QUADS_ACCEPTED).increment(1);
			Text outputKey = new Text(quad.getGraph().getURI() + "\t" + quad.getSubject().getURI());
			context.write(outputKey, value);
			LOG.debug("Emitting <g:s,quad> pair <{},{}> ", outputKey, value);
		}else{
			context.getCounter(RDF_SOLR_COUNTER_GROUP, QUADS_DROPPED).increment(1);
		}
	}
}


