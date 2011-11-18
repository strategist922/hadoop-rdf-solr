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

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.jena.tdbloader3.io.QuadWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talis.hadoop.rdf.QuadArrayWritable;

public class CollationReducer extends Reducer<Text, QuadWritable, Text, QuadArrayWritable> {

	private static final Logger LOG = LoggerFactory.getLogger(CollationReducer.class);

	private static final QuadWritable[] ARRAY = new QuadWritable[0];
	
	@Override
    public void setup(Context context) {
		LOG.info("Configuring Quad grouping reducer");
	}

	@Override
	public void reduce(Text key, Iterable<QuadWritable> value, Context context) throws IOException, InterruptedException {
		LOG.debug("Key is {}", key);
		QuadArrayWritable outputValue = collateQuads(value);
		context.write(key, outputValue);
	}

	private QuadArrayWritable collateQuads(Iterable<QuadWritable> input){
		Collection<QuadWritable> quads = new HashSet<QuadWritable>();
		for (QuadWritable qw : input ){
	        quads.add(new QuadWritable(qw.getQuad()));
		}
		LOG.debug("Collated {} quads", quads.size());
		QuadArrayWritable outputValue = new QuadArrayWritable(quads.toArray(ARRAY));
		return outputValue;
	}

}
