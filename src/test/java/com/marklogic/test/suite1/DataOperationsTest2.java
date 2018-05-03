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
import com.marklogic.client.document.XMLDocumentManager;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.InputStreamHandle;
import com.marklogic.client.io.SearchHandle;
import com.marklogic.client.query.DeleteQueryDefinition;
import com.marklogic.client.query.QueryManager;
import com.marklogic.client.query.StringQueryDefinition;

@Configuration
@PropertySource(value = { "classpath:DataOperations.properties",
		                  "classpath:user.properties" }, 
                           ignoreResourceNotFound = true)
public class DataOperationsTest2 extends AbstractApiTest {

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
	@Value("${XMLDocPath}")
	private String XML_DOC_PATH;
	@Value("${XMLDocCollectionSuffix}")
	private String XML_DOC_COLLECTION_SUFFIX;
	@Value("${namePrefix}")
	private String NAME_PREFIX;
	private Long TOTAL_XML_DOCS_ADDED = 0L;
	String DB_NAME = "";

	@After
	public void teardown() {
	}

	
	public void loadXMLDocuments(DatabaseClient client, String COLLECTION_NAME) throws Exception {

		File fl = new File(XML_DOC_PATH);
		
		GeneralUtils genUtils = new GeneralUtils();
		ArrayList<File> lstFiles = genUtils.listFilesForFolder(fl, false, ".*\\.xml");
		String fileName = "";

		for (int i = 0; i < lstFiles.size(); i++) {
			InputStream docStream = new FileInputStream(lstFiles.get(i));
			fileName = lstFiles.get(i).getName();

			// create a manager for XML documents
			XMLDocumentManager docMgr = client.newXMLDocumentManager();

			// create a handle on the content
			InputStreamHandle handle = new InputStreamHandle(docStream);
			DocumentMetadataHandle metadata = new DocumentMetadataHandle();
			// add a collection tag
			metadata.getCollections().addAll(COLLECTION_NAME);
			// write the document content
			docMgr.write("/" + COLLECTION_NAME + "_" + fileName, metadata, handle);
			TOTAL_XML_DOCS_ADDED++;

		}
	}

	public long countXMLDocuments(DatabaseClient client, String COLLECTION_NAME) {
		// create a manager for searching
		QueryManager queryMgr = client.newQueryManager();

		// create a search definition
		StringQueryDefinition query = queryMgr.newStringDefinition();

		// Restrict the search to the collection
		query.setCollections(COLLECTION_NAME);

		// create a handle for the search results
		SearchHandle resultsHandle = new SearchHandle();

		// run the search
		queryMgr.search(query, resultsHandle);
		return resultsHandle.getTotalResults();
	}

	public long deleteDocuments(DatabaseClient client, String COLLECTION_NAME) {
		// create a manager for searching
		QueryManager queryMgr = client.newQueryManager();

		DeleteQueryDefinition ddf = queryMgr.newDeleteDefinition();
		StringQueryDefinition query = queryMgr.newStringDefinition();

		ddf.setCollections(COLLECTION_NAME);
		queryMgr.delete(ddf);

		query.setCollections(COLLECTION_NAME);

		// create a handle for the search results
		SearchHandle resultsHandle = new SearchHandle();

		// run the search
		queryMgr.search(query, resultsHandle);

		return resultsHandle.getTotalResults();

	}

	@Test
	public void testLoadXMLDocuments() throws Exception {

		String prefix = java.util.UUID.randomUUID().toString();

		String methodName = new DataOperationsTest2() {
		}.getClass().getEnclosingMethod().getName();
		String className = this.getClass().getName();
		String COLLECTION_NAME = prefix + XML_DOC_COLLECTION_SUFFIX;
		GeneralUtils genTestUtils = new GeneralUtils();

		Date start = new Date();
		genTestUtils.logComments(start.toString() + " Started Test Case: " + className, LOGLEVEL);

		// Find DB Name
		DB_NAME = genTestUtils.getDBName(className, NAME_PREFIX);

		assertNotEquals("ERROR", DB_NAME);

		// Load some and count documents

		DatabaseClient client = DatabaseClientFactory.newClient(ML_HOST, 8000, DB_NAME,
				new DatabaseClientFactory.DigestAuthContext(ML_USER, ML_PASSWORD));
		loadXMLDocuments(client, COLLECTION_NAME);
		long countOfXMLDocs = countXMLDocuments(client, COLLECTION_NAME);
		assertEquals(TOTAL_XML_DOCS_ADDED.longValue(), countOfXMLDocs);
		genTestUtils.logComments(new Date().toString() + " Loaded " + TOTAL_XML_DOCS_ADDED + " documents.", LOGLEVEL);

		// countOfXMLDocs = deleteDocuments(client, COLLECTION_NAME);
		// assertEquals(0, countOfXMLDocs);
		// genTestUtils.logComments(new Date().toString() + " Deleted " +
		// TOTAL_XML_DOCS_ADDED + " documents.");

		Date end = new Date();
		genTestUtils.logComments(end.toString() + " Ended Test Case: " + className, LOGLEVEL);
		genTestUtils.logComments(
				"Execution time for " + className + " is " + (end.getTime() - start.getTime()) / 1000 + " seconds.",
				LOGLEVEL);
	}

}