package com.marklogic.test.suite1;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Date;

import org.junit.After;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.datamovement.DataMovementManager;
import com.marklogic.client.datamovement.JobTicket;
import com.marklogic.client.datamovement.QueryBatcher;
import com.marklogic.client.datamovement.WriteBatcher;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.FileHandle;
import com.marklogic.client.io.SearchHandle;
import com.marklogic.client.query.QueryManager;
import com.marklogic.client.query.StringQueryDefinition;

@Configuration
@PropertySource(value = { "classpath:DataOperations.properties",
		"classpath:user.properties" }, ignoreResourceNotFound = true)
public class JSONDMSDKOperationsTest4 extends AbstractApiTest {

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
	@Value("${JSONDocPath}")
	private String JSON_DOC_PATH;
	@Value("${JSONDocCollectionSuffix}")
	private String JSON_DOC_COLLECTION_SUFFIX;
	@Value("${namePrefix}")
	private String NAME_PREFIX;
	private Long TOTAL_JSON_DOCS_ADDED = 0L;
	private String DB_NAME = "";

	@After
	public void teardown() {

	}

	public void loadJSONDocuments(DatabaseClient client, String COLLECTION_NAME) throws Exception {

		DataMovementManager dmvMgr = client.newDataMovementManager();
		WriteBatcher batcher = dmvMgr.newWriteBatcher();
        batcher.withBatchSize(5)
        .withThreadCount(3)
        .onBatchSuccess(batch-> {
            TOTAL_JSON_DOCS_ADDED = batch.getJobWritesSoFar();
         })
        .onBatchFailure((batch,throwable) -> {
            throwable.printStackTrace();
         });
 
        final JobTicket ticket = dmvMgr.startJob(batcher);
        try {
            Files.walk(Paths.get(JSON_DOC_PATH))
                // .filter(Files::isRegularFile)
                 .filter(p -> p.getFileName().toString().endsWith(".json"))
                 .forEach(p -> {
                     String uri = "/" + ticket.getJobId() + "_" + p.getFileName().toString();
                   FileHandle handle = 
                       new FileHandle().with(p.toFile());
       			DocumentMetadataHandle metadata = new DocumentMetadataHandle();
    			// add a collection tag
    			metadata.getCollections().addAll(COLLECTION_NAME);
                batcher.add(uri, metadata, handle);
                 });
        } catch (IOException e) {
            e.printStackTrace();
        }
        
        // Start any partial batches waiting for more input, then wait
        // for all batches to complete. This call will block.

        batcher.flushAndWait();
        dmvMgr.stopJob(batcher);
        
		
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
	
	public long countJSONDocumentsUsingQueryBatcher(DatabaseClient client, String COLLECTION_NAME) {
		
		DataMovementManager dmvMgr = client.newDataMovementManager();
		QueryManager queryMgr = client.newQueryManager();
		// create a search definition
		StringQueryDefinition query = queryMgr.newStringDefinition();
		query.setCollections(COLLECTION_NAME);
		final QueryBatcher batcher = dmvMgr.newQueryBatcher(query)
		      .withBatchSize(10)
			  .withConsistentSnapshot();
		final JobTicket ticket = dmvMgr.startJob(batcher);
		batcher.awaitCompletion();
		dmvMgr.stopJob(ticket);
		return 0L; 
	}

	@Test
	public void testLoadJSONDocuments() throws Exception {

		String methodName = new JSONDMSDKOperationsTest4() {
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
		long countOfJsonDocs = countJSONDocumentsUsingQueryBatcher(client, COLLECTION_NAME);
		genTestUtils.logComments(new Date().toString() + " Loaded " + TOTAL_JSON_DOCS_ADDED + " documents.", LOGLEVEL);

		assertEquals(5, countOfJsonDocs);
		genTestUtils.logComments(new Date().toString() + " Loaded " + TOTAL_JSON_DOCS_ADDED + " documents.", LOGLEVEL);
		
		

		Date end = new Date();
		genTestUtils.logComments(end.toString() + " Ended Test Case: " + className, LOGLEVEL);
		genTestUtils.logComments(
				"Execution time for " + className + " is " + (end.getTime() - start.getTime()) / 1000 + " seconds.",
				LOGLEVEL);
	}

}