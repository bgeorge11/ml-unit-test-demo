package com.marklogic.test.suite1;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;

import org.junit.After;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.admin.QueryOptionsManager;
import com.marklogic.client.document.JSONDocumentManager;
import com.marklogic.client.expression.PlanBuilder;
import com.marklogic.client.io.DOMHandle;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.InputStreamHandle;
import com.marklogic.client.io.SearchHandle;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.io.ValuesHandle;
import com.marklogic.client.query.ExtractedItem;
import com.marklogic.client.query.ExtractedResult;
import com.marklogic.client.query.MatchDocumentSummary;
import com.marklogic.client.query.QueryDefinition;
import com.marklogic.client.query.QueryManager;
import com.marklogic.client.query.RawCombinedQueryDefinition;
import com.marklogic.client.query.StructuredQueryBuilder;
import com.marklogic.client.query.StructuredQueryDefinition;
import com.marklogic.client.query.ValuesDefinition;
import com.marklogic.client.row.RowManager;

@Configuration
@PropertySource(value = { "classpath:DataOperations.properties",
		"classpath:user.properties" }, ignoreResourceNotFound = true)
public class PartialExtractOperationsTest5 extends AbstractApiTest {

	@Value("${mlHost}")
	private String ML_HOST;
	@Value("${mlUser}")
	private String ML_USER;
	@Value("${mlPassword}")
	private String ML_PASSWORD;
	@Value("${logLevel}")
	private String LOGLEVEL;
	@Value("${mlRangeElementIndexLocalName}")
	private String RANGE_ELEMENT_INDEX_LOCAL_NAME;
	@Value("${mlRangeElementIndexScalarType}")
	private String RANGE_ELEMENT_INDEX_SCALAR_TYPE;
	@Value("${mlRangeElementIndexNS}")
	private String RANGE_ELEMENT_INDEX_NS;
	@Value("${pojoPath}")
	private String JSON_DOC_PATH;
	@Value("${JSONDocCollectionSuffix}")
	private String JSON_DOC_COLLECTION_SUFFIX;
	@Value("${namePrefix}")
	private String NAME_PREFIX;
	private Long TOTAL_JSON_DOCS_ADDED = 0L;
	private String DB_NAME = "";

	public void loadJSONDocuments(DatabaseClient client, String COLLECTION_NAME) throws Exception {

		File fl = new File(JSON_DOC_PATH);

		GeneralUtils genUtils = new GeneralUtils();

		ArrayList<File> lstFiles = genUtils.listFilesForFolder(fl, false, ".*\\.json");
		String fileName = "";

		for (int i = 0; i < lstFiles.size(); i++) {
			InputStream docStream = new FileInputStream(lstFiles.get(i));
			fileName = lstFiles.get(i).getName();

			// create a manager for JSON documents
			JSONDocumentManager docMgr = client.newJSONDocumentManager();

			// create a handle on the content
			InputStreamHandle handle = new InputStreamHandle(docStream);
			DocumentMetadataHandle metadata = new DocumentMetadataHandle();
			// add a collection tag
			metadata.getCollections().addAll(COLLECTION_NAME);
			// write the document content
			docMgr.write("/" + COLLECTION_NAME + "_" + fileName, metadata, handle);
			TOTAL_JSON_DOCS_ADDED++;

		}

	}

	public int getExtractedResultUsingQueryBuilder(DatabaseClient client, String COLLECTION_NAME) {
		GeneralUtils genTestUtils = new GeneralUtils();

		String searchOptions = 
				 "<search:options xmlns:search='http://marklogic.com/appservices/search'>" 
				+ "<search:extract-document-data>"
				+ "<search:extract-path>/employee/Addresses/Address/addrType</search:extract-path>" 
				+ "</search:extract-document-data>"
				+ "<search:additional-query>"
				+ "<cts:collection-query xmlns:cts=\"http://marklogic.com/cts\">"
				+ "<cts:uri>"+ COLLECTION_NAME + "</cts:uri>"
		        + " </cts:collection-query>"
		        + "</search:additional-query>"
				+ "</search:options>";
		
		
		StringHandle queryHandle = new StringHandle(searchOptions);
		/*
		 * It is always better to persist the query options,  here the installation of q
		 * query options is through the test class
		 */
		QueryOptionsManager qoManager=
	            client.newServerConfigManager().newQueryOptionsManager(); 
		qoManager.writeOptions("myoptions",queryHandle);
		DOMHandle qoHandle = qoManager.readOptions("myoptions", new DOMHandle());
		assertEquals("search:options",qoHandle.get().getFirstChild().getNodeName());
		
		/**** VALUES RETRIEVAL ****/
		StructuredQueryBuilder queryBuilder = new StructuredQueryBuilder();
		StructuredQueryDefinition query;
		query = queryBuilder.and(
				queryBuilder.value(queryBuilder.jsonProperty("lName"),"Kennedy"));
		// create a manager for searching
		QueryManager queryMgr = client.newQueryManager();
		String comboq = 
			    "<search xmlns=\"http://marklogic.com/appservices/search\">" +
			    		query.serialize() + 
			        "</search>";
		System.out.println("COMBO QUERY" + comboq);
		RawCombinedQueryDefinition rawQueryDef = 
				queryMgr.newRawCombinedQueryDefinition(
			        new StringHandle(comboq).withFormat(Format.XML), "myoptions");
		
		// retrieve the results
		SearchHandle results = queryMgr.search(rawQueryDef, new SearchHandle());
		MatchDocumentSummary[] matches = results.getMatchResults();
		for (MatchDocumentSummary match : matches) {
			ExtractedResult extracts = match.getExtracted();
			for (ExtractedItem extract: extracts) {
				genTestUtils.logComments("  extracted content: " +
                    extract.getAs(String.class), LOGLEVEL);
            }
		}
		
		qoManager.deleteOptions("myoptions");
	
		return matches.length ; // just returning the count of matches
		
	}
	
	public int getExtractedResultUsingQueryDefinition(DatabaseClient client, String COLLECTION_NAME) {
		
		GeneralUtils genTestUtils = new GeneralUtils();

		String searchOptions = "<search:search xmlns:search=\"http://marklogic.com/appservices/search\">" 
				+ "<cts:element-word-query xmlns:cts=\"http://marklogic.com/cts\">" 
				+ "<cts:element>lName</cts:element>"
				+ "<cts:text xml:lang=\"en\">Kennedy</cts:text>" 
				+ "</cts:element-word-query>" 
				+ "<search:options>" 
				+ "<search:extract-document-data>"
				+ "<search:extract-path>/employee/Addresses</search:extract-path>" 
				+ "</search:extract-document-data>"
				+ "<search:additional-query>"
				+ "<cts:collection-query xmlns:cts=\"http://marklogic.com/cts\">"
				+ "<cts:uri>"+ COLLECTION_NAME + "</cts:uri>"
		        + " </cts:collection-query>"
		        + "</search:additional-query>"
				+ "</search:options>"
				+  "</search:search>";
		
		StringHandle queryHandle = new StringHandle(searchOptions).withFormat(Format.XML);
		
		/**** VALUES RETRIEVAL ****/
		// create a manager for searching
		QueryManager queryMgr = client.newQueryManager();
		// create a values definition
		QueryDefinition query = queryMgr.newRawCombinedQueryDefinition(queryHandle);
		
		// retrieve the results
		SearchHandle results = queryMgr.search(query, new SearchHandle());
		MatchDocumentSummary[] matches = results.getMatchResults();
		for (MatchDocumentSummary match : matches) {
			ExtractedResult extracts = match.getExtracted();
			for (ExtractedItem extract: extracts) {
				genTestUtils.logComments("  extracted content: " +
                    extract.getAs(String.class), LOGLEVEL);
            }
		}
		
		return matches.length ; // just returning the count of matches

	}
	
	public long countJSONDocuments(DatabaseClient client, String COLLECTION_NAME) {

		String optionsName = "myOptions";
		QueryOptionsManager optionsMgr = client.newServerConfigManager().newQueryOptionsManager();

		String opts1 = "<search:options xmlns:search='http://marklogic.com/appservices/search'>"
				+ "<search:values name=\"uri\">" + "<search:uri/>" + "</search:values>" + "<search:additional-query>"
				+ "<cts:collection-query xmlns:cts=\"http://marklogic.com/cts\">" + "<cts:uri>" + COLLECTION_NAME
				+ "</cts:uri>" + " </cts:collection-query>" + "</search:additional-query>" + "</search:options>";

		StringHandle handle = new StringHandle(opts1);
		optionsMgr.writeOptions(optionsName, handle);
		/**** VALUES RETRIEVAL ****/
		// create a manager for searching
		QueryManager queryMgr = client.newQueryManager();

		// create a values definition
		ValuesDefinition valuesDef = queryMgr.newValuesDefinition("uri", optionsName);

		// retrieve the values
		ValuesHandle valuesHandle = queryMgr.values(valuesDef, new ValuesHandle());
		optionsMgr.deleteOptions(optionsName);

		return valuesHandle.getValues().length;
	}

	@Test
	public void testLoadJSONDocs() throws Exception {

		String methodName = new PartialExtractOperationsTest5() {
		}.getClass().getEnclosingMethod().getName();
		String className = this.getClass().getName();
		String prefix = java.util.UUID.randomUUID().toString();
		String COLLECTION_NAME = prefix + JSON_DOC_COLLECTION_SUFFIX;
		Date start = new Date();

		GeneralUtils genTestUtils = new GeneralUtils();
		genTestUtils.logComments(start.toString() + " Started Test Case: " + className, LOGLEVEL);

		// Find DB Name
		DB_NAME = genTestUtils.getDBName(className, NAME_PREFIX);
		assertNotEquals("ERROR", DB_NAME);
		// Load some and count documents

		DatabaseClient client = DatabaseClientFactory.newClient(ML_HOST, 8000, DB_NAME,
				new DatabaseClientFactory.DigestAuthContext(ML_USER, ML_PASSWORD));
		loadJSONDocuments(client, COLLECTION_NAME);
		long countOfJsonDocs = countJSONDocuments(client, COLLECTION_NAME);
		assertEquals(TOTAL_JSON_DOCS_ADDED.longValue(), countOfJsonDocs);
		genTestUtils.logComments(new Date().toString() + " Loaded " + TOTAL_JSON_DOCS_ADDED + " documents.", LOGLEVEL);
		assertEquals(1, getExtractedResultUsingQueryDefinition(client, COLLECTION_NAME));
		assertEquals(1, getExtractedResultUsingQueryBuilder(client, COLLECTION_NAME));
		Date end = new Date();
		genTestUtils.logComments(end.toString() + " Ended Test Case: " + className, LOGLEVEL);
		genTestUtils.logComments(
				"Execution time for " + className + " is " + (end.getTime() - start.getTime()) / 1000 + " seconds.",
				LOGLEVEL);
	}

}