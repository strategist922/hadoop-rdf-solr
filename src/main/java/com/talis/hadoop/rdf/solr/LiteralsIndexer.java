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
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.jena.tdbloader3.io.QuadWritable;
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

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.core.Quad;
import com.talis.hadoop.rdf.QuadArrayWritable;

public class LiteralsIndexer extends SolrDocumentConverter<Text, QuadArrayWritable>{

	private final static Logger LOG = LoggerFactory.getLogger(LiteralsIndexer.class);

	public Collection<SolrInputDocument> convert(Text key, QuadArrayWritable value) {
		try {
			String[] keyComponents = key.toString().split("\t");
			String graph = keyComponents[0];
			String subject = keyComponents[1]; 
			LOG.debug("Creating SolrInputDocument for graph {} and subject {}", graph, subject);
			notNull(graph, "Graph uri can not be null.");
			notNull(subject, "Subject uri can not be null.");
			notNull(value, "Quads can not be null.");
			
			SolrInputDocument doc = new SolrInputDocument();
			doc.setField(CoreFieldNames.MODIFIED_DATE,DateTools.dateToString(new Date(), DateTools.Resolution.SECOND));
			doc.setField(CoreFieldNames.DOCUMENT_KEY, documentKeyFor(graph, subject));
			doc.setField(CoreFieldNames.GRAPH_URI, graph);
			doc.setField(CoreFieldNames.SUBJECT_URI, subject);
			
			Collection<SolrInputDocument> documents = new ArrayList<SolrInputDocument>();
			for (Writable writable : value.get()) {
				if (! (writable instanceof QuadWritable)){
					continue;
				}
				Quad quad = ((QuadWritable)writable).getQuad();
				LOG.debug("Processing quad {}", quad);
				Node object = quad.getObject();
				if ( object.isLiteral() ) {
					doc.addField(quad.getPredicate().getURI(), object.getLiteralValue());
				}
				documents.add(doc);
			}
			
			return documents;
		} catch (RuntimeException e) {
			LOG.error("Exception while converting quads to solr document", e);
			throw e;
		}
	}
	
	private Collection<Quad> parseQuads(String text){
		LOG.debug("QUADS -> {}", text);
		final Collection<Quad> quads = new HashSet<Quad>();
		Sink<Quad> sink = new Sink<Quad>(){
			@Override
			public void send(Quad item) {
				quads.add(item);
			}
			@Override
			public void flush() {}
			@Override
			public void close() {}

		};
		Tokenizer tokenizer = TokenizerFactory.makeTokenizerASCII(text) ;
        LangNQuads parser = new LangNQuads(tokenizer, RiotLib.profile(Lang.NQUADS, null), sink) ;
        parser.parse();
        return quads;
	}

	public String documentKeyFor(String graphUri, String subjectUri) {
		return graphUri + " " + subjectUri;
	}
}
