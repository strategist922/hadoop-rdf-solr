package com.talis.hadoop.rdf.solr;

import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DummyReducer extends Reducer<Writable, Writable, Writable, Writable> {
	private static final Logger LOG = LoggerFactory.getLogger(DummyReducer.class);
	
	protected void reduce(Writable key, Iterable<Writable> values, Context context) throws IOException ,InterruptedException {
		for (Writable value : values){
			LOG.debug("K,V -> {} {}", key.getClass().getName(), value.getClass().getName());
			context.write(key,value);
		}
	};
}
