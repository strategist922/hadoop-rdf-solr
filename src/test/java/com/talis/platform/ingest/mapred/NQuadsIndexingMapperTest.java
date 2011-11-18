package com.talis.platform.ingest.mapred;


//import static org.easymock.EasyMock.anyObject;
//import static org.easymock.EasyMock.expect;
//import static org.easymock.EasyMock.expectLastCall;
//import static org.easymock.classextension.EasyMock.createNiceMock;
//import static org.easymock.classextension.EasyMock.createStrictMock;
//import static org.easymock.classextension.EasyMock.replay;
//import static org.easymock.classextension.EasyMock.verify;
//import static org.junit.Assert.assertTrue;
//
//import java.io.File;
//import java.io.FileWriter;
//import java.io.IOException;
//import java.util.UUID;
//
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapred.FileInputFormat;
//import org.apache.hadoop.mapred.FileOutputFormat;
//import org.apache.hadoop.mapred.JobConf;
//import org.apache.hadoop.mapred.OutputCollector;
//import org.apache.hadoop.mapred.Reporter;
//import org.junit.Before;
//import org.junit.Test;
//
//import com.google.inject.Guice;
//import com.hp.hpl.jena.sparql.core.Quad;
//import com.talis.platform.ingest.reporting.aws.SimpleDBReporter;

public class NQuadsIndexingMapperTest {

//	LongWritable inputKey;
//	OutputCollector<Text, Text> mockCollector;
//	Reporter mockReporter;
//	NQuadsIndexingMapper myMapper;
//	SimpleDBReporter mockSimpleDBReporter;
//	File myDownloadedCredentials;
//	File myWorkingDir;
//	File myOutputDir;
//	
//	@Before
//	public void setup() throws Exception {
//		NQuadsIndexingMapper.injector = Guice.createInjector(
//				new MockAwsModule());
//		
//		inputKey = new LongWritable(0);
//		mockCollector = createStrictMock(OutputCollector.class);
//		mockReporter = createStrictMock(Reporter.class);
//		replay(mockReporter);
//		
//		mockSimpleDBReporter = createNiceMock(SimpleDBReporter.class);
//		expect(mockSimpleDBReporter.getReportWhenRead()).andReturn(100);
//		replay(mockSimpleDBReporter);
//		MockReporterFactory.mockReporter = mockSimpleDBReporter;		
//
//		myDownloadedCredentials = new File(System.getProperty("java.io.tmpdir"),"aws.properties");
//		if (myDownloadedCredentials.exists()) {
//			assertTrue(myDownloadedCredentials.delete());
//		}
//		
//		myWorkingDir = new File(System.getProperty("java.io.tmpdir"), 
//				UUID.randomUUID().toString());
//		myWorkingDir.mkdirs();
//
//		myOutputDir = new File(myWorkingDir, "out");
//		myOutputDir.mkdirs();
//		
//		myMapper = new NQuadsIndexingMapper();
//		myMapper.configure(getJobConf());		
//	}
//	
//	private JobConf getJobConf() throws Exception {
//		JobConf conf = new JobConf();
//		conf.setJobName("test");
//		conf.set("uriTemplate","http://api.talis.com/{uid}");
//
//		File credentialsToDownload = new File(myWorkingDir,"credentialsForDownload");
//		FileWriter writer = new FileWriter(credentialsToDownload);
//		writer.write("Test Credentials");
//		writer.close();
//		conf.set("credentials",credentialsToDownload.getAbsolutePath());
//		
//		FileInputFormat.setInputPaths(conf, new Path(myWorkingDir.getAbsolutePath()));
//		FileOutputFormat.setOutputPath(conf, new Path(myOutputDir.getAbsolutePath()));
//		
//		return conf;
//	}
//	
//	
//	@Test
//	public void mapTripleWithURISubject() throws IOException{
//		Text inputValue = new Text("<http://example.org/things/1> " +
//										"<http://example.com/schema/prop1> " +
//											"\"literal value\" .");
//		mockCollector.collect(new Text("http://example.org/things/1"), inputValue);
//		replay(mockCollector);
//		myMapper.map(inputKey, inputValue, mockCollector, mockReporter);
//		verify(mockReporter);
//		verify(mockCollector);
//	}
//	
//	@Test
//	public void skipTripleWithBNodeSubject() throws IOException{
//		Text value = new Text("_:node0 " +
//									"<http://example.com/schema/prop1> " +
//										"\"literal value\" .");
//		
//		replay(mockCollector);
//		myMapper.map(inputKey, value, mockCollector, mockReporter);
//		verify(mockReporter);
//		verify(mockCollector);
//	}
//	
//	@Test
//	public void skipInvalidNTripleInput() throws IOException{
//		Text value = new Text("NOT VALID AS NTRIPLES");
//		replay(mockCollector);
//		myMapper.map(inputKey, value, mockCollector, mockReporter);
//		verify(mockReporter);
//		verify(mockCollector);
//	}
//	
//	@Test
//	public void quadsNotInDefaultGraphSkipped() throws IOException{
//		Text inputValue = new Text( "<http://example.org/things/1> " +
//									"<http://example.com/schema/prop1> " +
//									"\"literal value\" " +
//									"<http://example.com/graphs/1> .");
//		replay(mockCollector);
//		myMapper.map(inputKey, inputValue, mockCollector, mockReporter);
//		verify(mockReporter);
//		verify(mockCollector);
//	}
//	
//	@Test
//	public void quadsInDefaultGraphCollected() throws IOException{
//		Text inputValue = new Text( "<http://example.org/things/1> " +
//									"<http://example.com/schema/prop1> " +
//									"\"literal value\" " +
//									"<" + Quad.defaultGraphIRI + "> .");
//		mockCollector.collect(new Text("http://example.org/things/1"), inputValue);
//		replay(mockCollector);
//		myMapper.map(inputKey, inputValue, mockCollector, mockReporter);
//		verify(mockReporter);
//		verify(mockCollector);
//	}
//	
//	@Test
//	public void skipQuadWithBNodeSubject() throws IOException{
//		Text value = new Text(  "_:node0 " +
//								"<http://example.com/schema/prop1> " +
//								"\"literal value\" " +
//								"<http://example.com/graphs/1> .");
//		
//		replay(mockCollector);
//		myMapper.map(inputKey, value, mockCollector, mockReporter);
//		verify(mockReporter);
//		verify(mockCollector);
//	}
//
//	@Test
//	public void skipQuadWithBNodeGraph() throws IOException{
//		Text value = new Text(  "<http://example.org/things/1> " +
//								"<http://example.com/schema/prop1> " +
//								"\"literal value\" " +
//								"_:node0 .");
//		
//		replay(mockCollector);
//		myMapper.map(inputKey, value, mockCollector, mockReporter);
//		verify(mockReporter);
//		verify(mockCollector);
//	}
//	
//	@Test
//	public void progressReported() throws Exception{
//		
//		mockSimpleDBReporter = createStrictMock(SimpleDBReporter.class);
//		expect(mockSimpleDBReporter.getReportWhenRead()).andReturn(2).times(2);
//		mockSimpleDBReporter.report(
//				(String)anyObject(), 
//				(String)anyObject(), 
//				(String)anyObject(), 
//				(String)anyObject(), 
//				(String)anyObject());
//		expect(mockSimpleDBReporter.getReportWhenRead()).andReturn(2).times(2);
//		mockSimpleDBReporter.report(
//				(String)anyObject(), 
//				(String)anyObject(), 
//				(String)anyObject(), 
//				(String)anyObject(), 
//				(String)anyObject());
//		replay(mockSimpleDBReporter);
//		MockReporterFactory.mockReporter = mockSimpleDBReporter;		
//		
//		myMapper = new NQuadsIndexingMapper();
//		myMapper.configure(getJobConf());		
//		
//		Text inputValue = new Text("<http://example.org/things/1> " +
//										"<http://example.com/schema/prop1> " +
//											"\"literal value\" .");
//		
//		mockCollector = createStrictMock(OutputCollector.class);
//		mockCollector.collect(new Text("http://example.org/things/1"), inputValue);
//		expectLastCall().times(4);
//		replay(mockCollector);
//		
//		for (int i=0; i<4; i++) {
//			myMapper.map(inputKey, inputValue, mockCollector, mockReporter);
//		}
//		
//		verify(mockReporter);
//		verify(mockCollector);
//		verify(mockSimpleDBReporter);
//	}
//	
	
}
