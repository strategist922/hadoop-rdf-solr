package com.talis.hadoop.rdf.solr;

import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.solr.hadoop.SolrRecordWriter;

public class SolrReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
	    super.setup(context);
	    SolrRecordWriter.addReducerContext(context);
	}
	
}
