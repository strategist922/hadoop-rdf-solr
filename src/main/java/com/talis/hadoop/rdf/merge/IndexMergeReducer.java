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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
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
    private File combinedDir;
    
    private TaskAttemptID taskAttemptID;

	@Override
    public void setup(Context context) {
		LOG.info("Configuring index merge reducer");
		taskAttemptID = context.getTaskAttemptID();
		try{
			fs = FileSystem.get(FileOutputFormat.getOutputPath(context).toUri(), context.getConfiguration());
            outRemote = FileOutputFormat.getWorkOutputPath(context);
            LOG.info(String.format("Remote output path is %s", outRemote));
            
            String workDirRoot = context.getConfiguration().get(LOCAL_WORK_ROOT_DIR, System.getProperty("java.io.tmpdir"));
            LOG.info(String.format("Local work root directory is %s", workDirRoot));
            
            localWorkDir = new File(workDirRoot, context.getJobName() + "_" + context.getJobID() + "_" + taskAttemptID);
            FileUtils.forceMkdir(localWorkDir);
            LOG.info(String.format("Local work directory is %s", localWorkDir));
            
            localShards = new Path(localWorkDir.getAbsolutePath(), "shards");
            localShardsDir = new File(localShards.toString());
            FileUtils.forceMkdir(localShardsDir);
            LOG.info(String.format("Local shards directory is %s", localShardsDir));
            
            outLocal = new Path(localWorkDir.getAbsolutePath(), "combined");
            combinedDir = new File(outLocal.toString());
            FileUtils.forceMkdir(combinedDir);
            LOG.info(String.format("Local combined index directory is %s", combinedDir));
        } catch (Exception e) {
        	LOG.error("Error initialising merge reducer", e);
            throw new RuntimeException(e);
        }		
	}
	
	@Override
	public void cleanup(final Context context) throws IOException, InterruptedException {
		super.cleanup(context);

		Runnable reporter = new Runnable(){
			@Override
			public void run() {
				context.progress();
			}
		};
		ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        ScheduledFuture<?> task = scheduler.scheduleAtFixedRate(reporter, 60, 60, TimeUnit.SECONDS);
		LOG.info("Scheduled progress reporter, combining index shards");
		
		ArrayList<String> indexDirs = new ArrayList<String>();
		indexDirs.add(combinedDir.getAbsolutePath());
		for (File shardDir : localShardsDir.listFiles()){
			if (shardDir.isDirectory() && shardDir.getName().startsWith("part-")){
				indexDirs.add(shardDir.getAbsolutePath() + "/data/index");
			}
		}
		String[] args = new String[indexDirs.size()];
		args = indexDirs.toArray(args);
		LOG.info(String.format("About to combine %s shards", args.length - 1));
		
		/* 
		 * Copy/pasted from IndexMergeTool.main() as we want to remove the full merge it performs
		 */
		FSDirectory mergedIndex = FSDirectory.open(new File(args[0]));

	    IndexWriter writer = new IndexWriter(mergedIndex, new IndexWriterConfig(
	        Version.LUCENE_35, new WhitespaceAnalyzer(Version.LUCENE_35))
	        .setOpenMode(OpenMode.CREATE));

	    Directory[] indexes = new Directory[args.length - 1];
	    for (int i = 1; i < args.length; i++) {
	      indexes[i  - 1] = FSDirectory.open(new File(args[i]));
	    }

	    LOG.info("Merging...");
	    writer.addIndexes(indexes);
	    writer.close();
	    LOG.info("Done.");
		
		boolean disableZip = 
				context.getConfiguration().getBoolean(LEAVE_OUTPUT_UNZIPPED, false);
		if (disableZip){
			LOG.info("Transferring local output to remote filesystem");
			if ( fs != null ) {
				fs.completeLocalOutput(outRemote, outLocal);
			}
			LOG.info("Transfer completed");
		}else{
			if ( fs != null ) {
				Path pathToZip = new Path(outRemote, "solr.zip");
				ZipUtils.makeRemoteZip(pathToZip, outLocal, context.getConfiguration(), fs);
			}
		}
		LOG.info("Combined index built, terminating reporter");
		task.cancel(true);
	} 
	
	@Override
	public void reduce(LongWritable key, Iterable<Text> value, final Context context) throws IOException, InterruptedException {
        FileSystem shardsFs = null;
		for (Text remoteShard : value){
			Path remote = new Path(remoteShard.toString());
			if (null == shardsFs){
				shardsFs = FileSystem.get(remote.toUri(), context.getConfiguration());
			}
			LOG.info(String.format("Copying shard from %s to %s", remote, localShards));
			shardsFs.copyToLocalFile(remote, localShards);
			LOG.info("Copy complete");
		}
	}
}