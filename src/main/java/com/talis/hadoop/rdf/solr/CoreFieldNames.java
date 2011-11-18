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

package com.talis.hadoop.rdf.solr;

public class CoreFieldNames {
	public static final String DOCUMENT_KEY = "documentKey";
	public static final String GRAPH_URI = "graphUri";
	public static final String SUBJECT_URI = "subjectUri";
	public static final String RESOURCE_CONTENT = "xRcont";
	public static final String ANY_FIELD = "any";
	public static final String ID = "id";
	public static final String MODIFIED_DATE = "xMdate";  
	public static final String TYPE = "type";
	public static final String CONTENT_TYPE = "xCtype";  
	public static final String ETAG = "etag";
	
	public static final String LUCENE_SORTING_FIELD_POSTFIX = "sort";
	public static final String SOLR_SORTING_AND_FACETING_FIELD_POSTFIX = "_" + LUCENE_SORTING_FIELD_POSTFIX;
	
}
