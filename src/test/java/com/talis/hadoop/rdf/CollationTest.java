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

package com.talis.hadoop.rdf;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.jena.tdbloader3.AbstractMiniMRClusterTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.talis.hadoop.rdf.collation.QuadsCollater;

public class CollationTest extends AbstractMiniMRClusterTest {
	private String input;
	private String output;
	
	@Before
	public void setup() throws Exception{
		startCluster();
	  }

	@After
	public void teardown() throws Exception{
		stopCluster();
	}
	
	@Test
	public void test() throws Exception{
	    input = "src/test/resources/ntriples/100-test-ntriples.nt";
	    output = "output";

	    String[] args = new String[] {
                "-conf", config, 
                input, 
                output
        };		
	    ToolRunner.run(new QuadsCollater(), args); 
	    System.out.println("JOB COMPLETED, COPYING TO LOCAL FS");
	    fs.copyToLocalFile(new Path("output"), new Path("/tmp/foo"));
	    System.out.println("COPY COMPLETED");
	}

}
