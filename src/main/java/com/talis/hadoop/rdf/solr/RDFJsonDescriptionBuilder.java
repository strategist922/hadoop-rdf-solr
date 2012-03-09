package com.talis.hadoop.rdf.solr;

import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.openjena.atlas.lib.Pair;
import org.openjena.riot.out.SinkEntityOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.core.Quad;

public class RDFJsonDescriptionBuilder {

	private final static Logger LOG = LoggerFactory.getLogger(RDFJsonDescriptionBuilder.class);

	public String getDescriptionFor(Iterable<Quad> quads) {
		LOG.debug("Compiling writer input");
		Map<Node, Set<Node>> predicates = new HashMap<Node, Set<Node>>() ;
		Node subject = null;
		for(Quad quad : quads){
			if (null == subject){
				subject = quad.getSubject();
			}
			
			Triple triple =	quad.asTriple();
			Node p = triple.getPredicate() ;
			if ( predicates.containsKey(p) ) {
				predicates.get(p).add(triple.getObject()) ; 
			} else {
				Set<Node> objects = new HashSet<Node>() ;
				objects.add(triple.getObject()) ;
				predicates.put(p, objects) ;
			}
		}
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		SinkEntityOutput writer = new SinkEntityOutput(out);
		LOG.debug("Writing json description");
		writer.send(new Pair<Node, Map<Node,Set<Node>>>(subject, predicates));
		writer.flush();
		LOG.debug("Done writing json description");
		return out.toString();
	}

}
