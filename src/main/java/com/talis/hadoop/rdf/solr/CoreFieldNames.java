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
