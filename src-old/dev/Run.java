package dev;

import java.io.IOException;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrCore;
import org.apache.solr.schema.IndexSchema;
import org.xml.sax.SAXException;

public class Run {
	
	private static EmbeddedSolrServer server = null;
	private static CoreContainer container = null;

	public static void run() throws ParserConfigurationException, IOException, SAXException {
		String solrCoreName = "";
		String solrDataDirectory = "target/prova";
		String solrHomeDirectory = "solr";
		
	    System.setProperty("solr.solr.home", solrHomeDirectory);
		System.setProperty("solr.core.dataDir", solrDataDirectory);
		System.setProperty("solr.core.name", solrCoreName);

		container = new CoreContainer();
		SolrConfig solrConfig = new SolrConfig();
		IndexSchema indexSchema = new IndexSchema(solrConfig, null, null);
		SolrCore core = new SolrCore(solrDataDirectory, indexSchema);
		core.setName(solrCoreName);
		container.register(core, false);
		server = new EmbeddedSolrServer(container, solrCoreName);
	}
	
	public static void main(String[] args) throws SolrServerException, IOException, ParserConfigurationException, SAXException {
		run();

		// add one document to it
		SolrInputDocument doc = new SolrInputDocument();
		doc.addField("uri", "foo:bar");
		server.add(doc);
		server.commit(true, true);
		
		// search and count documents
		SolrQuery query = new SolrQuery();
		query.setQuery( "*:*" );
		QueryResponse response = server.query(query);
		System.out.println(response.getResults().getNumFound());
		
		// shutdown CoreContainer
		container.shutdown();
	}

}
