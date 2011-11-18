/**
 * 
 */
package com.talis.hadoop.rdf.solr;

import java.io.IOException;
import java.io.StringReader;

import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVStrategy;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.jena.tdbloader3.io.QuadWritable;

public class SolrMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

	private static final Log LOG = LogFactory.getLog(SolrMapper.class);

	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
	}

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		context.write(key, value);
	}
}
