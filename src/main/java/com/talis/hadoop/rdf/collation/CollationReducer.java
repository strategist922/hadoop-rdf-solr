package com.talis.hadoop.rdf.collation;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Collection;
import java.util.HashSet;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.jena.tdbloader3.io.QuadWritable;
import org.openjena.atlas.io.OutputUtils;
import org.openjena.riot.out.OutputLangUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CollationReducer extends Reducer<Text, QuadWritable, Text, Text> {

	private static final Logger LOG = LoggerFactory.getLogger(CollationReducer.class);

	private static final QuadWritable[] ARRAY = new QuadWritable[0];
	
	@Override
    public void setup(Context context) {
		LOG.info("Configuring Quad grouping reducer");
	}

	@Override
	public void reduce(Text key, Iterable<QuadWritable> value, Context context) throws IOException, InterruptedException {
		LOG.debug("Key is {}", key);
		Text outputValue = collateQuadsAsText(value);
		context.write(key, outputValue);
	}

	private Text collateQuadsAsText(Iterable<QuadWritable> input){
		StringBuffer buf = new StringBuffer();
		for (QuadWritable qw : input){
   		    StringWriter out = new StringWriter();
	        OutputLangUtils.output(out, qw.getQuad(), null, null);
	        buf.append(out.toString());
		}
		return new Text(buf.toString().replaceAll("\n", "@@EOQ@@"));
	}
	
	private ArrayWritable collateQuads(Iterable<QuadWritable> input){
		Collection<QuadWritable> quads = new HashSet<QuadWritable>();
		for (QuadWritable qw : input ){
	        quads.add(qw);
		}
		LOG.debug("Collated {} quads", quads.size());
		ArrayWritable outputValue = new ArrayWritable(QuadWritable.class);
		outputValue.set(quads.toArray(ARRAY));
		return outputValue;
	}

}
