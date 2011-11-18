package com.talis.hadoop.rdf.solr;

import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DummyMapper extends Mapper<Writable, Writable, Writable, Writable> {

	private static final Logger LOG = LoggerFactory.getLogger(DummyMapper.class);

	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
	}

	@Override
	public void map(Writable key, Writable value, Context context) throws IOException, InterruptedException {
		LOG.debug("K,V -> {} {}", key.getClass().getName(), value.getClass().getName());
		context.write(key,value);
	}

}
