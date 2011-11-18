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

package com.talis.platform.ingest.mapred;

//import static org.easymock.EasyMock.anyObject;
//import static org.easymock.EasyMock.expect;
//import static org.easymock.EasyMock.expectLastCall;
//import static org.easymock.classextension.EasyMock.createMock;
//import static org.easymock.classextension.EasyMock.createNiceMock;
//import static org.easymock.classextension.EasyMock.createStrictMock;
//import static org.easymock.classextension.EasyMock.replay;
//import static org.easymock.classextension.EasyMock.verify;
//import static org.junit.Assert.assertEquals;
//import static org.junit.Assert.assertNotNull;
//import static org.junit.Assert.assertNull;
//import static org.junit.Assert.assertTrue;

//import java.io.File;
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.Collection;
//import java.util.Iterator;
//import java.util.UUID;
//
//import org.apache.commons.io.FileUtils;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapred.JobConf;
//import org.apache.hadoop.mapred.OutputCollector;
//import org.apache.hadoop.mapred.Reporter;
//import org.apache.solr.client.solrj.SolrQuery;
//import org.apache.solr.client.solrj.SolrServer;
//import org.apache.solr.client.solrj.SolrServerException;
//import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
//import org.apache.solr.client.solrj.response.QueryResponse;
//import org.apache.solr.common.SolrDocument;
//import org.apache.solr.common.SolrInputDocument;
//import org.junit.After;
//import org.junit.Before;
//import org.junit.Test;
//import org.openrdf.model.Statement;
//import org.openrdf.model.URI;
//import org.openrdf.model.ValueFactory;
//import org.openrdf.model.impl.URIImpl;
//import org.openrdf.model.impl.ValueFactoryImpl;
//import org.openrdf.model.vocabulary.RDF;
//
//import com.google.inject.Guice;
//import com.talis.platform.indexing.DocumentBuilder;
//import com.talis.platform.indexing.FieldPredicateMap;
//import com.talis.platform.ingest.reporting.aws.SimpleDBReporter;

public class SolrIndexingReducerTest {

//	private File myWorkingDir;
//	private File myTempDir;
//	private File myOutputDir;
//	private String myMappingPath; 
//	private OutputCollector<Text, Text> myCollector;
//	private Reporter myReporter;
//	private String myDomainName;
//	private String mySiteId;
//	private String myIngestJobId;
//	private String myCredentialsPath;
//	File myDownloadedCredentials;
//	
//	@Before
//	public void setup(){
//		myWorkingDir = new File(System.getProperty("java.io.tmpdir"), 
//							UUID.randomUUID().toString());
//		myWorkingDir.mkdirs();
//		
//		myTempDir = new File(myWorkingDir, "temp");
//		myTempDir.mkdirs();
//		
//		myOutputDir = new File(myWorkingDir, "out");
//		myOutputDir.mkdirs();
//		
//		myCollector = createNiceMock(OutputCollector.class);
//		myReporter = createNiceMock(Reporter.class);
//		
//		myMappingPath = "./src/test/resources/multiple-mapping-fpmap-4.rdf";
//		myCredentialsPath = "./src/test/resources/test-aws.properties";
//		
//		SolrIndexingReducer.injector = Guice.createInjector(new MockAwsModule());
//		
//		myDomainName = "test-domain-01";
//		mySiteId = "test-site-01";
//		myIngestJobId = "test-ingestjobid-01";
//		
//		myDownloadedCredentials = new File(System.getProperty("java.io.tmpdir"),"aws.properties");
//		if (myDownloadedCredentials.exists()) {
//			assertTrue(myDownloadedCredentials.delete());
//		}
//		
//		System.setProperty(SolrIndexingReducer.SOLR_CONFIG_DIRECTORY_PROPERTY, "src/main/resources/solr");
//	}
//	
//	@After
//	public void tearDown(){
//		try{
//			FileUtils.deleteDirectory(myWorkingDir);
//		}catch(Exception e){
//			e.printStackTrace();
//		}
//		System.clearProperty(SolrIndexingReducer.SOLR_CONFIG_DIRECTORY_PROPERTY);
//	}
//	
//	@Test
//	public void initialiseIndexWriterInConfigure() throws IOException{
//		JobConf conf = createConfig();
//		SolrIndexingReducer myReducer = new SolrIndexingReducer();
//		assertNull(myReducer.getSolrServer());
//		myReducer.configure(conf);
//		
//		SolrServer server = myReducer.getSolrServer();
//		assertNotNull(server);
//		assertTrue(server instanceof EmbeddedSolrServer);
//		
//		myReducer.close();
//	}
//
//	private JobConf createConfig() {
//		JobConf conf = SolrIndexingJob.getConf(myWorkingDir.getAbsolutePath(), 
//										  myOutputDir.getAbsolutePath(), 
//										  myMappingPath, myDomainName, mySiteId, myIngestJobId, myCredentialsPath);
//		conf.set("mapred.work.output.dir", myOutputDir.getAbsolutePath()+File.separator+"_temporary");
//		return conf;
//	}
//	
//	@Test
//	public void loadMappingsInConfigure(){
//		JobConf conf = createConfig();
//		URI predicateURI = new URIImpl("http://example.com/schema#foo"); 
//		SolrIndexingReducer myReducer = new SolrIndexingReducer();
//		assertTrue(myReducer.getMappings().getShortNamesForPredicateURI(
//				predicateURI).isEmpty());
//		myReducer.configure(conf);
//		assertEquals("foo", 
//				myReducer.getMappings().getShortNamesForPredicateURI(
//						predicateURI).iterator().next());
//	}
//	
//	@Test
//	public void addDocumentsProviderByBuilderToIndex() throws IOException, SolrServerException{
//		JobConf conf = createConfig();
//		
//		ValueFactory vf = new ValueFactoryImpl();
//		final Statement firstStatement = 
//			vf.createStatement(vf.createURI("http://test.talis.com/things/1"),
//							   RDF.TYPE, 
//							   vf.createURI("http://test.talis.com/types/1"));
//		final Statement secondStatement = 
//			vf.createStatement(vf.createURI("http://test.talis.com/things/1"),
//							   RDF.TYPE, 
//							   vf.createURI("http://test.talis.com/types/2"));
//		Collection<Statement> statements = new ArrayList<Statement>(){{
//			add(firstStatement);
//			add(secondStatement);
//		}};
//		
//		String expectedValue = UUID.randomUUID().toString();
//		SolrInputDocument doc = new SolrInputDocument();
//		doc.addField("uri", expectedValue);
//		Collection<SolrInputDocument> documents = new ArrayList<SolrInputDocument>();
//		documents.add(doc);
//		
//		FieldPredicateMap mockMappings = createNiceMock(FieldPredicateMap.class);
//		replay(mockMappings);
//		
//		DocumentBuilder mockBuilder = createStrictMock(DocumentBuilder.class);
//		mockBuilder.getDocuments(statements, mockMappings);
//		expectLastCall().andReturn(documents);
//		replay(mockBuilder);
//		
//		final String firstTriple = "<http://test.talis.com/things/1> " +
//		"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " +
//		"<http://test.talis.com/types/1> . ";
//		final String secondTriple = "<http://test.talis.com/things/1> " +
//		"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " +
//		"<http://test.talis.com/types/2> . ";
//
//		Text key = new Text("http://test.talis.com/things/1");
//		Iterator<Text> values =	new ArrayList<Text>(){{ 
//										add(new Text(firstTriple));
//										add(new Text(secondTriple));
//								}}.iterator(); 
//
//		SolrIndexingReducer myReducer = new SolrIndexingReducer();
//		myReducer.configure(conf);
//		myReducer.setDocumentBuilder(mockBuilder);
//		myReducer.setPredicateMappings(mockMappings);
//		SolrServer server = myReducer.getSolrServer();
//	    SolrQuery query = new SolrQuery();
//	    query.setQuery( "*:*" );
//		QueryResponse response = server.query(query);
//		assertEquals(0, response.getResults().getNumFound());
//								
//		myReducer.reduce(key, values, myCollector, myReporter);
//		server.commit(true, true);
//
//		response = server.query(new SolrQuery().setQuery("*:*"));
//		assertEquals(1, response.getResults().getNumFound());
//		SolrDocument actualDoc = response.getResults().get(0);
//		assertEquals((String)doc.get("uri").getValue(), (String)actualDoc.get("uri"));
//		verify(mockBuilder);
//		
//		myReducer.close();
//		
//	}
//	
//	@Test
//	public void finalFiguresReported() throws Exception {
//
//		SimpleDBReporter mockReporter = createStrictMock(SimpleDBReporter.class);
//		mockReporter.report(
//				(String)anyObject(), 
//				(String)anyObject(), 
//				(String)anyObject(), 
//				(String)anyObject(), 
//				(String)anyObject());
//		expectLastCall().times(2);
//		replay(mockReporter);
//		MockReporterFactory.mockReporter = mockReporter;
//		
//		
//		JobConf conf = createConfig();
//		
//		Collection<Statement> statements = new ArrayList<Statement>();
//		Collection<SolrInputDocument> documents = new ArrayList<SolrInputDocument>();
//		
//		FieldPredicateMap mockMappings = createNiceMock(FieldPredicateMap.class);
//		replay(mockMappings);
//		
//		DocumentBuilder mockBuilder = createStrictMock(DocumentBuilder.class);
//		mockBuilder.getDocuments(statements, mockMappings);
//		expectLastCall().andReturn(documents);
//		replay(mockBuilder);
//		
//		Text key = new Text("http://test.talis.com/things/1");
//		Iterator<Text> values =	new ArrayList<Text>().iterator(); 
//
//		SolrIndexingReducer myReducer = new SolrIndexingReducer();
//		myReducer.configure(conf);
//		myReducer.setDocumentBuilder(mockBuilder);
//		myReducer.setPredicateMappings(mockMappings);
//								
//		myReducer.reduce(key, values, myCollector, myReporter);
//		myReducer.close();
//		myReducer.commit();
//		
//		verify(mockBuilder);
//		
//		verify(mockReporter);
//	}
//		
//	@Test
//	public void progressReported() throws Exception{
//		SimpleDBReporter mockReporter = createMock(SimpleDBReporter.class);
//		expect(mockReporter.getReportWhenRead()).andReturn(100).anyTimes();
//		mockReporter.report(
//				(String)anyObject(), 
//				(String)anyObject(), 
//				(String)anyObject(), 
//				(String)anyObject(), 
//				(String)anyObject());
//		expectLastCall().times(6);
//		replay(mockReporter);
//		MockReporterFactory.mockReporter = mockReporter;
//
//		int num_triples = 200;
//
//		JobConf conf = createConfig();
//
//		Collection<Statement> statements = new ArrayList<Statement>();
//		ValueFactory vf = new ValueFactoryImpl();
//
//		Collection<SolrInputDocument> documents = new ArrayList<SolrInputDocument>();
//
//		FieldPredicateMap mockMappings = createNiceMock(FieldPredicateMap.class);
//		replay(mockMappings);
//
//		DocumentBuilder mockBuilder = createStrictMock(DocumentBuilder.class);
//		mockBuilder.getDocuments((Collection<Statement>)anyObject(), (FieldPredicateMap)anyObject());
//		expectLastCall().andReturn(documents);
//		replay(mockBuilder);
//
//		Text key = new Text("http://test.talis.com/things/1");
//		ArrayList<Text> textArray =	new ArrayList<Text>(); 
//		for (int i = 0; i < num_triples; i++) {
//			final String triple = "<http://test.talis.com/things/1> " +
//			"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " +
//			"<http://test.talis.com/types/"+i+"> . ";
//			textArray.add(new Text(triple));
//		}
//		Iterator<Text> values = textArray.iterator();
//
//
//		SolrIndexingReducer myReducer = new SolrIndexingReducer();
//		myReducer.configure(conf);
//		myReducer.setDocumentBuilder(mockBuilder);
//		myReducer.setPredicateMappings(mockMappings);
//
//		myReducer.reduce(key, values, myCollector, myReporter);
//		myReducer.close();
//		myReducer.commit();
//
//		verify(mockBuilder);
//		verify(mockReporter);
//	}
//
//	@Test
//	public void credentialsDownloaded() throws IOException{
//		JobConf conf = createConfig();
//		URI predicateURI = new URIImpl("http://example.com/schema#foo"); 
//		SolrIndexingReducer myReducer = new SolrIndexingReducer();
//		myReducer.configure(conf);
//
//		assertTrue(myDownloadedCredentials.exists());
//	}
	
}
