/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Extracted from org.apache.solr.hadoop.SolrRecordWriter in the patch 
 * contributed to https://issues.apache.org/jira/browse/SOLR-1301 by
 * Andrzej Bialecki - https://issues.apache.org/jira/secure/ViewProfile.jspa?name=ab
 */

package com.talis.hadoop.rdf;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talis.io.SizeLimitedOutputStream;

public class ZipUtils {
	private static final Logger LOG = LoggerFactory.getLogger(ZipUtils.class);

	public static void makeLocalZip(File source, OutputStream out) throws IOException{
		ZipOutputStream zos = new ZipOutputStream(out);
		zipDir(source.getAbsolutePath(), source, zos);
		zos.flush();
		zos.close();
	}
	
	private static void zipDir(String root, File source, ZipOutputStream zos) throws IOException{
		for(File sourceFile : source.listFiles()) {
			if(sourceFile.isDirectory())	{
				zipDir(root, source, zos);
			}else{
				FileInputStream fis = new FileInputStream(sourceFile);
				ZipEntry anEntry = new ZipEntry(sourceFile.getPath().substring(root.length() + 1));
				zos.putNextEntry(anEntry);
				org.apache.commons.io.IOUtils.copyLarge(fis, zos);
				fis.close();
			}
		}
	}
	
	public static void makeRemoteZip(Path perm, Path temp, Configuration conf, FileSystem fs) throws IOException {
		FSDataOutputStream out = null;
		ZipOutputStream zos = null;
		int zipCount = 0;
		LOG.info("Packing zip file for " + perm);
		try {
			out = fs.create(perm, false);
			zos = new ZipOutputStream(out);
			String name = perm.getName().replaceAll(".zip$", "");
			LOG.info("adding index directory" + temp);
			zipCount = zipDirectory(conf, zos, "", temp.toString(), temp);
		} catch (Throwable t) {
			LOG.error("packZipFile exception", t);
			if (t instanceof RuntimeException) {
				throw (RuntimeException) t;
			}
			if (t instanceof IOException) {
				throw (IOException) t;
			}
			throw new IOException(t);
		} finally {
			if (zos != null) {
				if (zipCount == 0) { // If no entries were written, only close out, as
					// the zip will throw an error
					LOG.error("No entries written to zip file " + perm);
					fs.delete(perm, false);
					// out.close();
				} else {
					LOG.info(String.format("Wrote %d items to %s for %s", zipCount, perm,
							temp));
					zos.close();
				}
			}
		}
	}

	/**
	 * Write a file to a zip output stream, removing leading path name components
	 * from the actual file name when creating the zip file entry.
	 * 
	 * The entry placed in the zip file is <code>baseName</code>/
	 * <code>relativePath</code>, where <code>relativePath</code> is constructed
	 * by removing a leading <code>root</code> from the path for
	 * <code>itemToZip</code>.
	 * 
	 * If <code>itemToZip</code> is an empty directory, it is ignored. If
	 * <code>itemToZip</code> is a directory, the contents of the directory are
	 * added recursively.
	 * 
	 * @param zos The zip output stream
	 * @param baseName The base name to use for the file name entry in the zip
	 *        file
	 * @param root The path to remove from <code>itemToZip</code> to make a
	 *        relative path name
	 * @param itemToZip The path to the file to be added to the zip file
	 * @return the number of entries added
	 * @throws IOException
	 */
	static public int zipDirectory(final Configuration conf,
			final ZipOutputStream zos, final String baseName, final String root,
			final Path itemToZip) throws IOException {
		LOG.info("zipDirectory: {} {} {}", new Object[]{baseName, root, itemToZip});
		LocalFileSystem localFs = FileSystem.getLocal(conf);
		int count = 0;

		final FileStatus itemStatus = localFs.getFileStatus(itemToZip);
		if (itemStatus.isDir()) {
			final FileStatus[] statai = localFs.listStatus(itemToZip);

			// Add a directory entry to the zip file
			final String zipDirName = relativePathForZipEntry(itemToZip.toUri()
					.getPath(), baseName, root);
			final ZipEntry dirZipEntry = new ZipEntry(zipDirName + Path.SEPARATOR_CHAR);
			LOG.info(String.format("Adding directory %s to zip", zipDirName));
			zos.putNextEntry(dirZipEntry);
			zos.closeEntry();
			count++;

			if (statai == null || statai.length == 0) {
				LOG.info(String.format("Skipping empty directory %s", itemToZip));
				return count;
			}
			for (FileStatus status : statai) {
				count += zipDirectory(conf, zos, baseName, root, status.getPath());
			}
			LOG.info(String.format("Wrote %d entries for directory %s", count,
					itemToZip));
			return count;
		}

		final String inZipPath = relativePathForZipEntry(itemToZip.toUri()
				.getPath(), baseName, root);

		if (inZipPath.length() == 0) {
			LOG.warn(String.format("Skipping empty zip file path for %s (%s %s)",
					itemToZip, root, baseName));
			return 0;
		}

		// Take empty files in case the place holder is needed
		FSDataInputStream in = null;
		try {
			in = localFs.open(itemToZip);
			final ZipEntry ze = new ZipEntry(inZipPath);
			ze.setTime(itemStatus.getModificationTime());
			// Comments confuse looking at the zip file
			// ze.setComment(itemToZip.toString());
			zos.putNextEntry(ze);

			IOUtils.copyBytes(in, zos, conf, false);
			zos.closeEntry();
			LOG.info(String.format("Wrote %d entries for file %s", count, itemToZip));
			return 1;
		} finally {
			in.close();
		}

	}

	static String relativePathForZipEntry(final String rawPath,
			final String baseName, final String root) {
		String relativePath = rawPath.replaceFirst(Pattern.quote(root.toString()),
				"");
		LOG.info(String.format("RawPath %s, baseName %s, root %s, first %s",
				rawPath, baseName, root, relativePath));

		if (relativePath.startsWith(Path.SEPARATOR)) {
			relativePath = relativePath.substring(1);
		}
		LOG.info(String.format(
				"RawPath %s, baseName %s, root %s, post leading slash %s", rawPath,
				baseName, root, relativePath));
		if (relativePath.isEmpty()) {
			LOG.warn(String.format(
					"No data after root (%s) removal from raw path %s", root, rawPath));
			return baseName;
		}
		// Construct the path that will be written to the zip file, including
		// removing any leading '/' characters
		String inZipPath = baseName + Path.SEPARATOR_CHAR + relativePath;

		LOG.info(String.format("RawPath %s, baseName %s, root %s, inZip 1 %s",
				rawPath, baseName, root, inZipPath));
		if (inZipPath.startsWith(Path.SEPARATOR)) {
			inZipPath = inZipPath.substring(1);
		}
		LOG.info(String.format("RawPath %s, baseName %s, root %s, inZip 2 %s",
				rawPath, baseName, root, inZipPath));

		return inZipPath;

	}
	public static void main(String[] args) throws IOException{
		File combinedDir = new File("/tmp/idx");
		OutputStream archiveOut = new SizeLimitedOutputStream(new File("/tmp/solr.zip"), 1024);
		ZipUtils.makeLocalZip(combinedDir, archiveOut);
		LOG.info("Zip output completed, transferring to remote fs");
		
	}

}
