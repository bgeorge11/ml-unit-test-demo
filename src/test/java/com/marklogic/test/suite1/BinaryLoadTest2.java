package com.marklogic.test.suite1;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.regex.Pattern;

import org.junit.After;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.document.BinaryDocumentManager;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.InputStreamHandle;
import com.marklogic.client.io.SearchHandle;
import com.marklogic.client.query.DeleteQueryDefinition;
import com.marklogic.client.query.QueryManager;
import com.marklogic.client.query.StringQueryDefinition;

@Configuration
@PropertySource(value = { "classpath:DataOperations.properties",
		"classpath:user.properties" }, ignoreResourceNotFound = true)
public class BinaryLoadTest2 extends AbstractApiTest {

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
	@Value("${BinaryDocPath}")
	private String BINARY_DOC_PATH;
	@Value("${BinaryDocCollectionSuffix}")
	private String BINARY_DOC_COLLECTION_SUFFIX;
	@Value("${namePrefix}")
	private String NAME_PREFIX;
	private Long TOTAL_BINARY_DOCS_ADDED = 0L;
	private String DB_NAME = "";

	@After
	public void teardown() {

	}

	public static ArrayList<File> listFilesForFolder(final File folder, final boolean recursivity,
			final String patternFileFilter) {

		// Inputs
		boolean filteredFile = false;

		// Ouput
		final ArrayList<File> output = new ArrayList<File>();

		// Foreach elements
		for (final File fileEntry : folder.listFiles()) {

			// If this element is a directory, do it recursivly
			if (fileEntry.isDirectory()) {
				if (recursivity) {
					output.addAll(listFilesForFolder(fileEntry, recursivity, patternFileFilter));
				}
			} else {
				// If there is no pattern, the file is correct
				if (patternFileFilter.length() == 0) {
					filteredFile = true;
				}
				// Otherwise we need to filter by pattern
				else {
					filteredFile = Pattern.matches(patternFileFilter, fileEntry.getName());
				}

				// If the file has a name which match with the pattern, then add
				// it to the list
				if (filteredFile) {
					output.add(fileEntry);
				}
			}
		}

		return output;
	}

	public void loadBinaryDocuments(DatabaseClient client, String COLLECTION_NAME) throws Exception {

		File fl = new File(BINARY_DOC_PATH);

		ArrayList<File> lstFiles = listFilesForFolder(fl, false, "([^\\s]+(\\.(?i)(jpg|png|gif|bmp))$)");
		String fileName = "";

		for (int i = 0; i < lstFiles.size(); i++) {
			InputStream docStream = new FileInputStream(lstFiles.get(i));
			fileName = lstFiles.get(i).getName();

			// create a manager for Binary documents
			BinaryDocumentManager docMgr = client.newBinaryDocumentManager();

			// enable automatic metadata extraction into properties
			/*
			 * TODO Commented below as Marklogic converters should be installed
			 * for meta data extraction Effective marklogic 9, the converter is
			 * not available by default
			 * 
			 */
			// docMgr.setMetadataExtraction(MetadataExtraction.PROPERTIES);

			// create a handle on the document's content
			InputStreamHandle handle = new InputStreamHandle(docStream);

			DocumentMetadataHandle metadata = new DocumentMetadataHandle();
			// add a collection tag
			metadata.getCollections().addAll(COLLECTION_NAME);
			// write the document content
			docMgr.write("/" + COLLECTION_NAME + "_" + fileName, metadata, handle);
			TOTAL_BINARY_DOCS_ADDED++;
		}
	}

	public long countJSONDocuments(DatabaseClient client, String COLLECTION_NAME) {
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
	public void testloadBinaryDocuments() throws Exception {

		String methodName = new JSONDataOperationsTest2() {
		}.getClass().getEnclosingMethod().getName();
		String className = this.getClass().getName();
		String prefix = java.util.UUID.randomUUID().toString();
		String COLLECTION_NAME = prefix + BINARY_DOC_COLLECTION_SUFFIX;
		Date start = new Date();

		GeneralUtils genTestUtils = new GeneralUtils();
		genTestUtils.logComments(start.toString() + " Started Test Case: " + className, LOGLEVEL);

		// Find DB Name
		DB_NAME = genTestUtils.getDBName(className, NAME_PREFIX);
		assertNotEquals("ERROR", DB_NAME);
		// Load some and count documents

		DatabaseClient client = DatabaseClientFactory.newClient(ML_HOST, 8000, DB_NAME,
				new DatabaseClientFactory.DigestAuthContext(ML_USER, ML_PASSWORD));
		loadBinaryDocuments(client, COLLECTION_NAME);
		long countOfBinaryDocs = countJSONDocuments(client, COLLECTION_NAME);
		assertEquals(TOTAL_BINARY_DOCS_ADDED.longValue(), countOfBinaryDocs);
		genTestUtils.logComments(new Date().toString() + " Loaded " + TOTAL_BINARY_DOCS_ADDED + " documents.",
				LOGLEVEL);

		// countOfBinaryDocs = deleteDocuments(client, COLLECTION_NAME);
		// assertEquals(0, countOfBinaryDocs);
		// genTestUtils.logComments(new Date().toString() + " Deleted " +
		// TOTAL_BINARY_DOCS_ADDED + " documents.", LOGLEVEL);
		//
		Date end = new Date();
		genTestUtils.logComments(end.toString() + " Ended Test Case: " + className, LOGLEVEL);
		genTestUtils.logComments(
				"Execution time for " + className + " is " + (end.getTime() - start.getTime()) / 1000 + " seconds.",
				LOGLEVEL);
	}

}