package com.talis.platform.indexing;

//import static org.easymock.EasyMock.expectLastCall;
//import static org.easymock.classextension.EasyMock.createStrictMock;
//import static org.easymock.classextension.EasyMock.replay;
//import static org.easymock.classextension.EasyMock.verify;
//import static org.junit.Assert.assertEquals;
//import static org.junit.Assert.assertNotNull;
//import static org.junit.Assert.assertNull;
//import static org.junit.Assert.assertTrue;
//import static org.junit.Assert.fail;
//
//import java.io.IOException;
//import java.io.InputStream;
//import java.util.ArrayList;
//import java.util.Collection;
//import java.util.Date;
//import java.util.Iterator;
//
//import org.apache.lucene.document.DateTools;
//import org.apache.solr.common.SolrInputDocument;
//import org.apache.solr.common.SolrInputField;
//import org.junit.Before;
//import org.junit.Test;
//import org.openrdf.model.Statement;
//import org.openrdf.rio.RDFHandlerException;
//import org.openrdf.rio.RDFParseException;
//import org.openrdf.rio.RDFParser;
//import org.openrdf.rio.helpers.StatementCollector;
//import org.openrdf.rio.ntriples.NTriplesParserFactory;
//
//import com.talis.platform.ingest.mapred.SolrFieldNameEncoder;

public class DocumentBuilderTest {

//	private Collection<Quad> myStatements;
//	private DocumentBuilder myBuilder;
//	private FieldPredicateMap myMappings;
//	private DateProvider myDateProvider;
//	private long myTimestamp;
//	
//	@Before
//	public void setup() throws Exception{
//		myStatements = new ArrayList<Statement>();
//		myMappings = new FieldPredicateMap();
//		myMappings.loadRdfXml( 
//				this.getClass().getResourceAsStream("/multiple-mapping-fpmap-2.rdf"));
//
//		myTimestamp = System.currentTimeMillis();
//		myDateProvider = createStrictMock(DateProvider.class);
//		myDateProvider.getCurrentDate();
//		expectLastCall().andReturn(new Date(myTimestamp)).anyTimes();
//		replay(myDateProvider);
//		myBuilder = new DocumentBuilder(myDateProvider);
//		
//	}
//	
//	  @Test (expected=IllegalArgumentException.class)
//	  public void statementCollectionIsNull() throws Exception{
//	    myStatements = null;
//	    myBuilder.getDocuments(myStatements, myMappings);
//	  }
//	  
//	  @Test (expected=IllegalArgumentException.class)
//	  public void mappingIsNull() throws Exception{
//	    myMappings = null;
//	    myBuilder.getDocuments(myStatements, myMappings);
//	  }
//	  
//	  @Test
//	  public void usesShortNameForPredicatesInMapping() throws Exception {
//		reloadMappings("/multiple-mapping-fpmap-3.rdf");
//	    myStatements = readStatementsFromFile("/one-resource.nt");
//	    
//	    Collection<SolrInputDocument> docs = 
//	    	myBuilder.getDocuments(myStatements, myMappings);
//	    assertEquals(1, docs.size());
//	    SolrInputDocument doc = docs.iterator().next();
//	    
//	    Collection<Object> values = doc.getFieldValues(SolrFieldNameEncoder.encodeLuceneFieldNameForSolr(myMappings, "foo"));
//	    assertEquals("Incorrect number or fields in doc", 1, values.size());
//	    assertEquals("Incorrect value in field", "jin", (String)values.iterator().next());
//	  }
//	  
//	  	  @Test
//	  public void indexesASortFieldShortNameForPredicatesInMapping() throws Exception{
//	  	reloadMappings("/multiple-mapping-fpmap-3.rdf");
//		myStatements = readStatementsFromFile("/one-resource.nt");
//		    	    
//	    Collection<SolrInputDocument> docs = myBuilder.getDocuments(myStatements, myMappings);
//	    assertEquals(1, docs.size());
//	    SolrInputDocument doc = docs.iterator().next();
//	    
//	    Collection<Object> sortValues = doc.getFieldValues(myBuilder.getSortFieldNameForField("foo"));
//	    assertEquals(1, sortValues.size());
//	    assertEquals("jin", (String)sortValues.iterator().next());
//	  }
//	  
//	  @Test
//	  public void resourceValuesNotAddedToDocument() throws Exception{
//		reloadMappings("/multiple-mapping-fpmap-3.rdf");
//		myStatements = readStatementsFromFile("/one-resource.nt");
//	    Collection<SolrInputDocument> docs = myBuilder.getDocuments(myStatements, myMappings);
//	    assertEquals(1, docs.size());
//	    SolrInputDocument doc = docs.iterator().next();
//	    
//	    Collection<Object> values = doc.getFieldValues(SolrFieldNameEncoder.encodeLuceneFieldNameForSolr(myMappings, "bar"));
//	    assertNull("Incorrect number or fields in doc", values);
//	  }
//	  
//	  @Test
//	  public void resourceSortValuesNotAddedToDocument() throws Exception{
//		reloadMappings("/multiple-mapping-fpmap-3.rdf");
//		myStatements = readStatementsFromFile("/one-resource.nt");
//	    Collection<SolrInputDocument> docs = myBuilder.getDocuments(myStatements, myMappings);
//	    assertEquals(1, docs.size());
//	    SolrInputDocument doc = docs.iterator().next();
//	    Collection<Object> values = doc.getFieldValues(myBuilder.getSortFieldNameForField("bar"));
//	    assertNull("Incorrect number or fields in doc", values);
//	  }
//	  
//	  @Test
//	  public void subjectUriAddedToDocument() throws Exception{
//		reloadMappings("/multiple-mapping-fpmap-3.rdf");
//		myStatements = readStatementsFromFile("/one-resource.nt");
//	    Collection<SolrInputDocument> docs = myBuilder.getDocuments(myStatements, myMappings);
//	    assertEquals(1, docs.size());
//	    SolrInputDocument doc = docs.iterator().next();
//	    assertEquals( "http://example.com/res/1", 
//	    				(String)doc.get(Constants.URI_FIELD_NAME).getValue());
//	  }
//	  
//	  @Test
//	  public void modifiedDateSuppliedByProviderAddedToDocument() throws Exception{
//		reloadMappings("/multiple-mapping-fpmap-3.rdf");
//		myStatements = readStatementsFromFile("/one-resource.nt");
//	    Collection<SolrInputDocument> docs = myBuilder.getDocuments(myStatements, myMappings);
//	    assertEquals(1, docs.size());
//	    SolrInputDocument doc = docs.iterator().next();
//	    SolrInputField modifiedDateField = doc.getField(Constants.MDATE_FIELD_NAME); 
//	    assertNotNull( modifiedDateField );
//	    String actualModifiedDate = (String)modifiedDateField.getValue();
//	    assertEquals(
//	    		DateTools.dateToString(myDateProvider.getCurrentDate(),
//	    								DateTools.Resolution.SECOND),
//	    		actualModifiedDate);
//	    verify(myDateProvider);
//	  }
//	  
//	  @Test
//	  public void modelContainingMoreThanSingleTopLevelResourceIsOk() 
//	  throws Exception{
//		reloadMappings("/multiple-mapping-fpmap-3.rdf");
//		myStatements = readStatementsFromFile("/two-resources.nt");
//	    Collection<SolrInputDocument> docs = myBuilder.getDocuments(myStatements, myMappings);
//	    assertEquals(2, docs.size());
//	  }
//	  
//	  @Test
//	  public void sortFieldsLiteralsIndexedForModelContainingMoreThanOneTopLevelResource()
//	  throws Exception{
//		reloadMappings("/multiple-mapping-fpmap-3.rdf");
//		myStatements = readStatementsFromFile("/two-resources.nt");
//	    Collection<SolrInputDocument> docs = myBuilder.getDocuments(myStatements, myMappings);
//	    assertEquals(2, docs.size());
//	    Iterator<SolrInputDocument> iter = docs.iterator();
//	    while (iter.hasNext()){
//	    	SolrInputDocument doc = iter.next();
//	    	assertNotNull(doc.getField(myBuilder.getSortFieldNameForField("foo")));
//	    	assertNotNull(doc.getField(myBuilder.getSortFieldNameForField("baz")));
//	    	assertNull(doc.getField(myBuilder.getSortFieldNameForField("bar")));
//	    }
//	  }
//	  
//	  @Test
//	  public void addUntokenizedFieldForResourceSingleValuesOfRdfTypeWhereNoMappingPresent() throws Exception{
//		reloadMappings("/multiple-mapping-fpmap-3.rdf");
//		myStatements = readStatementsFromFile("/one-resource.nt");
//		Collection<SolrInputDocument> docs = myBuilder.getDocuments(myStatements, myMappings);
//		SolrInputDocument doc = docs.iterator().next();
//		SolrInputField typeField = doc.getField(Constants.RDF_TYPE_FIELD_NAME);
//	    assertEquals("Incorrect value in field", 
//	    			"http://example.com/things/1", 
//	    			(String)typeField.getValue());
//	  }
//	  
//	  @Test
//	  public void addSortFieldForFacetingForRdfType() throws Exception{
//		reloadMappings("/multiple-mapping-fpmap-3.rdf");
//		myStatements = readStatementsFromFile("/one-resource.nt");
//		Collection<SolrInputDocument> docs = myBuilder.getDocuments(myStatements, myMappings);
//		SolrInputDocument doc = docs.iterator().next();
//		SolrInputField typeField = doc.getField(Constants.RDF_TYPE_FIELD_NAME + Constants.SOLR_SORTING_AND_FACETING_FIELD_POSTFIX);
//	    assertEquals("Incorrect value in field", 
//	    			"http://example.com/things/1", 
//	    			(String)typeField.getValue());
//	  }
//	  
//	  @Test
//	  public void addUntokenizedFieldForResourceMultipleValuesOfRdfTypeWhereNoMappingPresent() throws Exception{
//		reloadMappings("/multiple-mapping-fpmap-3.rdf");
//		myStatements = readStatementsFromFile("/one-resource-with-multiple-types.nt");
//		Collection<SolrInputDocument> docs = myBuilder.getDocuments(myStatements, myMappings);
//		SolrInputDocument doc = docs.iterator().next();
//	    Collection<Object> values = doc.getFieldValues(Constants.RDF_TYPE_FIELD_NAME);
//	    assertEquals("Incorrect number or fields in doc", 2, values.size());
//	    assertTrue("Incorrect value in field", 
//	    		values.contains("http://example.com/things/1") );
//	    assertTrue("Incorrect value in field", 
//	    		values.contains("http://example.com/things/2") );
//	  }
//	  	  
//	  private void reloadMappings(String mappingFileName){
//		  try {
//			  myMappings.loadRdfXml(
//					  this.getClass().getResourceAsStream(mappingFileName));
//		  } catch (Exception e) {
//			  fail(String.format("Unable to initialise Mappings from file %s", 
//					  mappingFileName));
//		  }
//	  }
//
//	  public Collection<org.openrdf.model.Statement> 
//	  readStatementsFromStream(InputStream in) 
//	  throws RDFParseException, RDFHandlerException, IOException{
//
//		  StatementCollector collector = new StatementCollector();
//		  RDFParser parser = new NTriplesParserFactory().getParser();
//		  parser.setRDFHandler(collector);
//		  parser.parse(in, "");
//		  in.close();
//		  return collector.getStatements();
//	  }
//
//	  public Collection<org.openrdf.model.Statement> 
//	  readStatementsFromFile(String filename) 
//	  throws RDFParseException, RDFHandlerException, IOException{
//
//		  InputStream in = 
//			  this.getClass().getResourceAsStream(filename);
//
//		  return readStatementsFromStream(in);
//	  }
	
}
