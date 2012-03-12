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
import java.io.OutputStream;
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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.lucene.analysis.StopAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talis.hadoop.rdf.ZipUtils;
import com.talis.io.SizeLimitedOutputStream;

public class IndexMergeReducer extends Reducer<LongWritable, Text, LongWritable, Text>{

	private static final Logger LOG = LoggerFactory.getLogger(IndexMergeReducer.class);
	
	public static final String LOCAL_WORK_ROOT_DIR = "com.talis.hadoop.rdf.solr.merge.local.work";
	public static final String LEAVE_OUTPUT_UNZIPPED = "com.talis.hadoop.rdf.solr.merge.nozip";
	public static final String OPTIMIZE_OUTPUT = "com.talis.hadoop.rdf.solr.merge.optimize";

	private static final long MAX_ARCHIVE_SIZE = FileUtils.ONE_GB;
	
	private FileSystem fs;
    private Path outRemote;
    private Path outLocal;
    
    private File localWorkDir;
    private Path localShards;
    private File localShardsDir;
    private File combinedDir;
    
    private TaskAttemptID taskAttemptID;

    private Directory combined;
    private IndexWriter writer;
    
    private boolean optimizeOutput;
    
	@Override
    public void setup(Context context) {
		System.out.println("Configuring index merge reducer");
		taskAttemptID = context.getTaskAttemptID();
		try{
			fs = FileSystem.get(FileOutputFormat.getOutputPath(context).toUri(), context.getConfiguration());
            outRemote = FileOutputFormat.getWorkOutputPath(context);
            System.out.println(String.format("Remote output path is %s", outRemote));
            
            String workDirRoot = context.getConfiguration().get(LOCAL_WORK_ROOT_DIR, System.getProperty("java.io.tmpdir"));
            System.out.println(String.format("Local work root directory is %s", workDirRoot));
            
            localWorkDir = new File(workDirRoot, context.getJobName() + "_" + context.getJobID() + "_" + taskAttemptID);
            FileUtils.forceMkdir(localWorkDir);
            System.out.println(String.format("Local work directory is %s", localWorkDir));
            
            localShards = new Path(localWorkDir.getAbsolutePath(), "shards");
            localShardsDir = new File(localShards.toString());
            FileUtils.forceMkdir(localShardsDir);
            System.out.println(String.format("Local shards directory is %s", localShardsDir));
            
            outLocal = new Path(localWorkDir.getAbsolutePath(), "combined");
            combinedDir = new File(outLocal.toString());
            FileUtils.forceMkdir(combinedDir);
            System.out.println(String.format("Local combined index directory is %s", combinedDir));
            
            optimizeOutput = context.getConfiguration().getBoolean(OPTIMIZE_OUTPUT, true);
//            optimizeOutput = false;
            System.out.println(String.format("Output optimization false is set to %s", optimizeOutput));
            
            combined = FSDirectory.open(combinedDir);
            writer = new IndexWriter(combined, new StopAnalyzer(Version.LUCENE_33), 
					  true, IndexWriter.MaxFieldLength.UNLIMITED);
//            writer = new IndexWriter(combined,
//            			new IndexWriterConfig(Version.LUCENE_29, 
//            							new WhitespaceAnalyzer(Version.LUCENE_29))
//						.setOpenMode(OpenMode.CREATE));
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
		System.out.println("Scheduled progress reporter, combining index shards");
		
		Directory[] shards = getDirectories();
		System.out.println(String.format("About to combine %s shards", shards.length));
		writer.addIndexesNoOptimize(shards);
		if (optimizeOutput){
			System.out.println("Optimizing combined index");
			writer.optimize();
		}
		
		System.out.println("Optimization finished, committing and closing");
		writer.commit();
		System.out.println("Committed");
		writer.close();
		System.out.println("Closed");
		
		boolean disableZip = 
				context.getConfiguration().getBoolean(LEAVE_OUTPUT_UNZIPPED, false);
		if (disableZip){
			System.out.println("Transferring local output to remote filesystem");
			if ( fs != null ) {
				fs.completeLocalOutput(outRemote, outLocal);
			}
			System.out.println("Transfer completed");
		}else{
//			System.out.println("Making Zip archive");
//			File zipDir = new File(localWorkDir, "zip");
//			FileUtils.forceMkdir(zipDir);
//			OutputStream archiveOut = new SizeLimitedOutputStream(new File(zipDir, "solr.zip"), MAX_ARCHIVE_SIZE);
//			ZipUtils.makeLocalZip(combinedDir, archiveOut);
//			System.out.println("Zip output completed, transferring to remote fs");
//			outLocal = new Path(localWorkDir.getAbsolutePath(), "zip");
//			if ( fs != null ) {
//				fs.completeLocalOutput(outRemote, outLocal);
//			}
			
			if ( fs != null ) {
				Path pathToZip = new Path(outRemote, "solr.zip");
				ZipUtils.makeRemoteZip(pathToZip, outLocal, context.getConfiguration(), fs);
			}
			
		}
		System.out.println("Combined index built, terminating reporter");
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
			System.out.println(String.format("Copying shard from %s to %s", remote, localShards));
			shardsFs.copyToLocalFile(remote, localShards);
			System.out.println("Copy complete");
		}
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
				System.out.println("Contents of shard directory %s ");
				for (String f : directory.list()){
					System.out.println(f);
				}
				return false;
			}else{
				return true;
			}
		}
	}
}