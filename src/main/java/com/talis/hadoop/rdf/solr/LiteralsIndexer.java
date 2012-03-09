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

package com.talis.hadoop.rdf.solr;

import static org.apache.commons.lang.Validate.notNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.UUID;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.jena.tdbloader4.io.QuadWritable;
import org.apache.lucene.document.DateTools;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.hadoop.SolrDocumentConverter;
import org.openjena.atlas.lib.Sink;
import org.openjena.riot.Lang;
import org.openjena.riot.lang.LangNQuads;
import org.openjena.riot.system.RiotLib;
import org.openjena.riot.tokens.Tokenizer;
import org.openjena.riot.tokens.TokenizerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.core.Quad;
import com.talis.hadoop.rdf.QuadArrayWritable;

public class LiteralsIndexer extends SolrDocumentConverter<Text, QuadArrayWritable>{

	private final static Logger LOG = LoggerFactory.getLogger(LiteralsIndexer.class);

	private static final Function<Writable, Quad> TRANSFORM_WRITABLE_TO_QUAD = new Function<Writable, Quad>(){
		@Override
		public Quad apply(Writable input) {
			return ((QuadWritable)input).getQuad();
		}
	};
	
	private EmbeddedDescriptionDocumentBuilder documentBuilder;
	private String id = UUID.randomUUID().toString();
	private int docCount = 0;
	
	public LiteralsIndexer(){
		documentBuilder = new EmbeddedDescriptionDocumentBuilder();
	}
	
	public Collection<SolrInputDocument> convert(Text key, QuadArrayWritable value) {
		try {
			String subject = key.toString();
			LOG.info("{} Creating SolrInputDocument for subject {}", id, subject);
			notNull(subject, "Subject uri can not be null.");
			notNull(value, "Quads can not be null.");
			Iterable<Quad> quads = Iterables.transform(Arrays.asList(value.get()), 
														TRANSFORM_WRITABLE_TO_QUAD);
			SolrInputDocument doc = documentBuilder.getDocument(subject, quads);
			Collection<SolrInputDocument> documents = new ArrayList<SolrInputDocument>();
			documents.add(doc);
//			LOG.info("{} built document(s)", id, ++docCount);
			return documents;
		} catch (RuntimeException e) {
			LOG.error("Exception while converting quads to solr document", e);
			throw e;
		}
	}
}
