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

package com.talis.hadoop.rdf.merge;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.DirectoryWalker;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.jena.tdbloader3.TDBLoader3Exception;
import org.apache.lucene.analysis.StopAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talis.hadoop.rdf.ZipUtils;

public class IndexMergeReducer extends Reducer<LongWritable, Text, LongWritable, Text>{

	private static final Logger LOG = LoggerFactory.getLogger(IndexMergeReducer.class);
	
	public static final String LOCAL_WORK_ROOT_DIR = "com.talis.hadoop.rdf.solr.merge.local.work";
	public static final String LEAVE_OUTPUT_UNZIPPED = "com.talis.hadoop.rdf.solr.merge.nozip";
	public static final String OPTIMIZE_OUTPUT = "com.talis.hadoop.rdf.solr.merge.optimize";
	
	private FileSystem fs;
    private Path outRemote;
    private Path outLocal;
    
    private File localWorkDir;
    private Path localShards;
    private File localShardsDir;
    
    private TaskAttemptID taskAttemptID;

    private Directory combined;
    private IndexWriter writer;
    
    private boolean optimizeOutput;
    
	@Override
    public void setup(Context context) {
		LOG.info("Configuring index merge reducer");
		taskAttemptID = context.getTaskAttemptID();
		try {
            fs = FileSystem.get(FileOutputFormat.getOutputPath(context).toUri(), context.getConfiguration());
            outRemote = FileOutputFormat.getWorkOutputPath(context);
            LOG.debug("Remote output path is {}", outRemote);
            
            String workDirRoot = context.getConfiguration().get(LOCAL_WORK_ROOT_DIR, System.getProperty("java.io.tmpdir"));
            LOG.debug("Local work root directory is {}", workDirRoot);
            
            localWorkDir = new File(workDirRoot, context.getJobName() + "_" + context.getJobID() + "_" + taskAttemptID);
            FileUtils.forceMkdir(localWorkDir);
            LOG.info("Local work directory is {}", localWorkDir);
            
            localShards = new Path(localWorkDir.getAbsolutePath(), "shards");
            localShardsDir = new File(localShards.toString());
            FileUtils.forceMkdir(localShardsDir);
            LOG.info("Local shards directory is {}", localShardsDir);
            
            outLocal = new Path(localWorkDir.getAbsolutePath(), "combined");
            File combinedDir = new File(outLocal.toString());
            FileUtils.forceMkdir(combinedDir);
            LOG.info("Local combined index directory is {}", combinedDir);
            
            optimizeOutput = context.getConfiguration().getBoolean(OPTIMIZE_OUTPUT, true);
            LOG.info("Output optimization false is set to {}", optimizeOutput);
            
            combined = FSDirectory.open(combinedDir);
            writer = new IndexWriter(combined, new StopAnalyzer(Version.LUCENE_29), 
            						  true, IndexWriter.MaxFieldLength.UNLIMITED);
            
        } catch (Exception e) {
            throw new TDBLoader3Exception(e);
        }		
	}
	
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		super.cleanup(context);
		if (optimizeOutput){
			LOG.info("Optimizing combined index");
			writer.optimize();
		}
		LOG.info("Optimization finished, committing and closing");
		writer.commit();
		LOG.info("Committed");
		writer.close();
		LOG.info("Closed");
		
		boolean disableZip = 
				context.getConfiguration().getBoolean(LEAVE_OUTPUT_UNZIPPED, false);
		if (disableZip){
			LOG.info("Transferring local output to remote filesystem");
			if ( fs != null ) {
				fs.completeLocalOutput(outRemote, outLocal);
			}
			LOG.info("Transfer completed");
		}else{
			LOG.info("Zipping output on remote filesystem");
			if ( fs != null ) {
				Path pathToZip = new Path(outRemote, "solr.zip");
				ZipUtils.packZipFile(pathToZip, outLocal, context.getConfiguration(), fs);
			}
			LOG.info("Zip output completed");
		}
	} 
	
	@Override
	public void reduce(LongWritable key, Iterable<Text> value, final Context context) throws IOException, InterruptedException {
		Runnable reporter = new Runnable(){
			@Override
			public void run() {
				context.progress();
			}
		};
		ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        ScheduledFuture<?> task = scheduler.scheduleAtFixedRate(reporter, 60, 60, TimeUnit.SECONDS);
		LOG.debug("Scheduled progress reporter, combining index shards");
		
        FileSystem shardsFs = null;
		for (Text remoteShard : value){
			Path remote = new Path(remoteShard.toString());
			if (null == shardsFs){
				shardsFs = FileSystem.get(remote.toUri(), context.getConfiguration());
			}
			LOG.debug("Copying shard from {} to {}", remote, localShards);
			shardsFs.copyToLocalFile(remote, localShards);
			LOG.debug("Copy complete");
		}
		
		Directory[] shards = getDirectories();
		LOG.debug("About to combine {} shards", shards.length);
		writer.addIndexesNoOptimize(shards);
		LOG.debug("Combined index built, terminating reporter");
		task.cancel(true);
		
	}
	
	private Directory[] getDirectories() throws IOException{
		return new IndexDirectoryFinder().findIndexes();
	}	
		
	class IndexDirectoryFinder extends DirectoryWalker{
		Directory[] findIndexes() throws IOException{
			List<Directory> list = new ArrayList<Directory>();
			this.walk(localShardsDir, list);
			return list.toArray(new Directory[list.size()]);
		}
		
		@Override
		protected boolean handleDirectory(File directory, int depth, Collection results) throws IOException {
			if (directory.getName().equals("index")){
				results.add(FSDirectory.open(directory));
				return false;
			}else{
				return true;
			}
		}
	}
}
