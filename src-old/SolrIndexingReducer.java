package com.talis.hadoop.rdf.solr;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.jena.tdbloader3.TDBLoader3Exception;
import org.apache.jena.tdbloader3.io.QuadWritable;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrCore;
import org.apache.solr.schema.IndexSchema;
import org.openjena.riot.Lang;
import org.openjena.riot.lang.LangNQuads;
import org.openjena.riot.system.RiotLib;
import org.openjena.riot.tokens.Tokenizer;
import org.openjena.riot.tokens.TokenizerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.sparql.core.Quad;

import static com.talis.hadoop.rdf.Constants.*;


public class SolrIndexingReducer extends Reducer<Text, Text, NullWritable, NullWritable> {

	private static final Logger LOG = LoggerFactory.getLogger(SolrIndexingReducer.class);
	
	public static final String SOLR_CONFIG_DIRECTORY_PROPERTY = "com.talis.platform.ingest.mapred.SolrIndexingReducer.config.directory";
	public static final String DEFAULT_SOLR_CONFIG_PROPERTY = "solr";
	
	private FileSystem fs;
	private Path outLocal;
    private Path outRemote;
    private TaskAttemptID taskAttemptID;

	private LiteralsIndexer documentBuilder;
	private CoreContainer container;
	private EmbeddedSolrServer server;
	
	@Override
    public void setup(Context context) {
		this.taskAttemptID = context.getTaskAttemptID();
		try{
			LOG.info("Configuring Reducer");
			try {
	            fs = FileSystem.get(FileOutputFormat.getOutputPath(context).toUri(), context.getConfiguration());
	            outRemote = FileOutputFormat.getWorkOutputPath(context);
	            LOG.info("outRemote is {}", outRemote);
	            outLocal = new Path("/tmp", context.getJobName() + "_" + context.getJobID() + "_" + taskAttemptID);
	            LOG.debug("outLocal is {}", outLocal);
	            fs.startLocalOutput(outRemote, outLocal);
	        } catch (Exception e) {
	            throw new TDBLoader3Exception(e);
	        }			
			documentBuilder = new LiteralsIndexer();
		    String solrHomeDirectory = System.getProperty(SOLR_CONFIG_DIRECTORY_PROPERTY, DEFAULT_SOLR_CONFIG_PROPERTY);
		    String solrDataDirectory = outRemote.toString() + "/" + taskAttemptID;
		    LOG.info("Solr Data Directory is {}", solrDataDirectory);
		    String solrCoreName = "";
			initEmbeddedSolrServer(solrHomeDirectory, solrDataDirectory, solrCoreName);
		} catch (Exception e) {
			LOG.error("Caught Exception when initialising Reducer", e);
		}
	}

	private void initEmbeddedSolrServer (String solrHomeDirectory, String solrDataDirectory, String solrCoreName) throws Exception{
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
	
	private Collection<Quad> collectQuads(Iterable<Text> input){
		Collection<Quad> quads = new HashSet<Quad>();
		for (Text text : input ){
			Tokenizer tokenizer = TokenizerFactory.makeTokenizerASCII(new String(text.toString())) ;
	        LangNQuads parser = new LangNQuads(tokenizer, RiotLib.profile(Lang.NQUADS, null), null) ;
	        quads.add(parser.next());
		}
		return quads;
	}
	
	@Override
	public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
		Collection<Quad> statements = collectQuads(value);
		String[] keyParts = key.toString().split("\t");
		SolrInputDocument document = documentBuilder.getDocument(keyParts[0], keyParts[1], statements);
		LOG.debug("Key {} with {} quads produced document", key, statements.size());
		try {
			server.add(document);
			context.getCounter(RDF_SOLR_COUNTER_GROUP, DOCUMENTS_EMITTED).increment(1);
			server.commit(true, true);
		} catch (SolrServerException e) {
			LOG.error("Error indexing document", e);
		}
		context.write(NullWritable.get(), NullWritable.get());
	}
	
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		super.cleanup(context);
//		try {
//			server.optimize(true, true, 1);
			server = null;
			container.shutdown();
			if ( fs != null ) fs.completeLocalOutput(outRemote, outLocal);
//		} catch (SolrServerException e) {
//			LOG.error("Error committing changes to Solr index", e);
//		}
	}
}
