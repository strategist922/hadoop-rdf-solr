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

import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.sparql.core.Quad;
import com.talis.rdf.solr.DefaultDocumentBuilder;

public class EmbeddedDescriptionDocumentBuilder extends DefaultDocumentBuilder{

	private final static Logger LOG = LoggerFactory.getLogger(EmbeddedDescriptionDocumentBuilder.class);

	public static final String JSON_DESCRIPTION_FIELD = "json_description";  
	
	private final RDFJsonDescriptionBuilder descriptionBuilder;	
	
	public EmbeddedDescriptionDocumentBuilder() {
		this(new RDFJsonDescriptionBuilder());
	}
	
	public EmbeddedDescriptionDocumentBuilder(RDFJsonDescriptionBuilder descriptionBuilder) {
		this.descriptionBuilder = descriptionBuilder;
	}

	@Override
	public SolrInputDocument getDocument(String subject, Iterable<Quad> quads) {
		LOG.debug("Creating SolrInputDocument for subject {}", subject);
		SolrInputDocument doc = super.getDocument(subject, quads);
		LOG.debug("Describing {} as json", subject);
		String jsonDescription = descriptionBuilder.getDescriptionFor(quads);
		LOG.debug("Built json {}", jsonDescription);
		doc.addField(JSON_DESCRIPTION_FIELD, jsonDescription);		
		return doc;
	}
}
