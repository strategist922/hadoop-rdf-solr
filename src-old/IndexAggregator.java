package com.talis.platform.ingest;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;

public class IndexAggregator {

	static final Logger LOG = LoggerFactory.getLogger(IndexAggregator.class);
	public static final String USE_COMPUND_FILES_PROPERTY = "com.talis.platform.ingest.indexaggregator.usecompoundfiles";
	
	private static final Function<File, Directory> MAKE_FS_DIRECTORY 
		= new Function<File, Directory>(){
			@Override
			public Directory apply(File file) {
				try {
					File index = new File (file, "index");
					if ( index.exists() && index.isDirectory() && index.canRead() ) {
						// This is where Solr puts Lucene indexes
						return FSDirectory.getDirectory(index);
					} else {
						return FSDirectory.getDirectory(file);
					}
				} catch (IOException e) {
					String message = 
						String.format("Unable to create FSDirectory from %s", 
									  file);
					LOG.error(message, e);
					throw new RuntimeException(message, e);
				}
			}
		};
	
	public void aggregate(File shardsDir, File outputDir) 
	throws IOException{

		IndexWriter writer = new IndexWriter(outputDir, 
										new StandardAnalyzer(), true);
		
		boolean compound = Boolean.getBoolean(USE_COMPUND_FILES_PROPERTY); 
		LOG.info(String.format("Setting use compound file format to: %s", compound));
		writer.setUseCompoundFile(compound);

		LOG.info("Compiling list of shards");
		Collection<File> files = 
				Arrays.asList(shardsDir.listFiles(
						(FileFilter) DirectoryFileFilter.DIRECTORY));
		
		Collection<Directory> shards = 
				Collections2.transform(files, MAKE_FS_DIRECTORY);
		Directory[] shardsArray = shards.toArray(new Directory[0]);
		
		LOG.info("Done compiling list of shards, constructing combined index");
		writer.addIndexesNoOptimize(shardsArray);
		
		LOG.info("Combined indexes, optimizing");
		writer.optimize();

		LOG.info("Optimization complete, closing IndexWriter");		
		writer.close();

		IndexReader reader = IndexReader.open(outputDir);
		LOG.info(String.format("Combined index contains %s docs",
								reader.numDocs()));
		reader.close();
	}

	public static void main(String[] args) throws IOException {
		
		if (args.length != 2) {
			System.out.println("Invalid number of arguments!");
			System.exit(1);
		}
		
		File shardsDir = new File(args[0]);
		if (shardsDir.exists() == false || shardsDir.isDirectory() == false) {
			System.out.println("Invalid shards directory!");
			System.exit(1);
		}
		
		File outputDir = new File(args[1]);
		if (outputDir.exists() == false || outputDir.isDirectory() == false) {
			System.out.println("Invalid output directory!");
			System.exit(1);
		}
		
		IndexAggregator indexAggregator = new IndexAggregator();
		indexAggregator.aggregate(shardsDir, outputDir);
	}
	
}
