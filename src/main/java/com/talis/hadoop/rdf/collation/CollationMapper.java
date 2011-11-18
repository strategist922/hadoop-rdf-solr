package com.talis.hadoop.rdf.collation;

import java.io.IOException;
import java.io.StringWriter;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.jena.tdbloader3.io.QuadWritable;
import org.openjena.riot.out.OutputLangUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.core.Quad;

import static com.talis.hadoop.rdf.Constants.*;

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
//			StringWriter out = new StringWriter();
//			OutputLangUtils.output(out, quad, null, null);
			
			context.write(outputKey, value);
			LOG.debug("Emitting <g:s,quad> pair <{},{}> ", outputKey, value);
		}else{
			context.getCounter(RDF_SOLR_COUNTER_GROUP, QUADS_DROPPED).increment(1);
		}
	}
}


