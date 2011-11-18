package com.talis.platform.ingest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.LockObtainFailedException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class IndexAggregatorTest {

	private File tmpDir;
	private File inputDir;
	private File outputDir;

	@Before
	public void setUp() throws Exception {
		tmpDir = new File(System.getProperty("java.io.tmpdir"), UUID.randomUUID().toString());
		inputDir = new File (tmpDir, "input");
		outputDir = new File (tmpDir, "output");
	}

	@After
	public void tearDown() throws Exception {
		if (null != tmpDir && tmpDir.exists()){
			FileUtils.cleanDirectory(tmpDir);
			FileUtils.forceDelete(tmpDir);
		}
	}
	
	@Test
	public void testAggregateLuceneLayout() throws IOException {
		IndexAggregator aggregator = new IndexAggregator();
		assertNotNull(aggregator);
		
		int num_indexes = 4; 
		int num_documents = 3;
		for ( int i = 0; i < num_indexes; i++ ) {
			File path = new File (inputDir, "index_" + i); 
			createIndex(num_documents, path);
			assertEquals(num_documents, count(path));
		}

		aggregator.aggregate(inputDir, outputDir);
		assertEquals(num_indexes * num_documents, count(outputDir));
	}

	@Test
	public void testAggregateSolrLayout() throws IOException {
		IndexAggregator aggregator = new IndexAggregator();
		assertNotNull(aggregator);
		
		int num_indexes = 4; 
		int num_documents = 3;
		for ( int i = 0; i < num_indexes; i++ ) {
			File path = new File (new File (inputDir, "index_" + i), "index"); 
			createIndex(num_documents, path);
			assertEquals(num_documents, count(path));
		}

		aggregator.aggregate(inputDir, outputDir);
		assertEquals(num_indexes * num_documents, count(outputDir));
	}
	
	@Test
	public void testCompoundFilesFormatIsUsedByDefault() throws IOException {
		System.clearProperty(IndexAggregator.USE_COMPUND_FILES_PROPERTY);

		IndexAggregator aggregator = new IndexAggregator();
		assertNotNull(aggregator);
		
		int num_indexes = 4; 
		int num_documents = 3;
		for ( int i = 0; i < num_indexes; i++ ) {
			File path = new File (new File (inputDir, "index_" + i), "index"); 
			createIndex(num_documents, path);
			assertEquals(num_documents, count(path));
		}

		aggregator.aggregate(inputDir, outputDir);
		assertEquals(num_indexes * num_documents, count(outputDir));
		
		assertFalse (useCompountFilesFormat(outputDir));
	}

	@Test
	public void testCompoundFilesFormatIsConfigurableViaSystemProperty() throws IOException {
		try {
			System.setProperty(IndexAggregator.USE_COMPUND_FILES_PROPERTY, "true");
			
			IndexAggregator aggregator = new IndexAggregator();
			assertNotNull(aggregator);
			
			int num_indexes = 4; 
			int num_documents = 3;
			for ( int i = 0; i < num_indexes; i++ ) {
				File path = new File (new File (inputDir, "index_" + i), "index"); 
				createIndex(num_documents, path);
				assertEquals(num_documents, count(path));
			}

			aggregator.aggregate(inputDir, outputDir);
			assertEquals(num_indexes * num_documents, count(outputDir));
			
			assertTrue (useCompountFilesFormat(outputDir));
		} finally {
			System.clearProperty(IndexAggregator.USE_COMPUND_FILES_PROPERTY);
		}
	}

	private void createIndex(int n, File path) throws IOException {
		IndexWriter writer = new IndexWriter(path, new StandardAnalyzer(), true);
		for ( int i = 0; i < n; i++ ) {
			Document doc = new Document();
			doc.add(new Field("foo", "bar", Field.Store.NO, Field.Index.TOKENIZED));
			writer.addDocument(doc);			
		}
		writer.close();
	}
	
	private int count(File path) throws IOException {
		Directory dir = FSDirectory.getDirectory(path);
		IndexReader reader = IndexReader.open(dir);
		return reader.numDocs();
	}
	
	private boolean useCompountFilesFormat(File path) throws CorruptIndexException, LockObtainFailedException, IOException {
		return ( path.listFiles().length == 3 );
	}

}
