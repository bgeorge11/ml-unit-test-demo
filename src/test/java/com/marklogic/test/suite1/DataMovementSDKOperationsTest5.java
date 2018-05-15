package com.marklogic.test.suite1;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Date;

import org.junit.After;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.datamovement.DataMovementManager;
import com.marklogic.client.datamovement.JobTicket;
import com.marklogic.client.datamovement.WriteBatcher;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.SearchHandle;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.query.QueryManager;
import com.marklogic.client.query.StringQueryDefinition;

@Configuration
@PropertySource(value = { "classpath:DataOperations.properties",
		"classpath:user.properties" }, ignoreResourceNotFound = true)
public class DataMovementSDKOperationsTest5 extends AbstractApiTest {

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

	public void loadCSV(DatabaseClient client, String COLLECTION_NAME) throws Exception {

		GeneralUtils genTestUtils = new GeneralUtils();
		File dir = new File(JSON_DOC_PATH);
		
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
        ObjectMapper csvMapper;
        csvMapper = new CsvMapper();
        ObjectMapper mapper = new ObjectMapper();
        CsvSchema bootstrapSchema;
        bootstrapSchema = CsvSchema.builder()
        					.addColumn("id")
        					.addColumn("ANON_MBR_ID")
        					.addColumn("MBR_AGE")
        					.addColumn("SEX_CD")
        					.addColumn("SRC_CUST_ANON_ID")
        					.addColumn("SRC_ASO_IND")
        					.addColumn("MAJOR_LOB_CD")
        					.addColumn("fin_prod_cd")
        					.addColumn("FIN_SUB_CD")
        					.addColumn("SOLD_LEDGER_NBR")
        					.addColumn("DOCUMENT_KEY")
        					.addColumn("ndc_anon_id")
        					.addColumn("PAY_DAY_SUPPLY_CNT")
        					.addColumn("BILL_AMT")
        					.addColumn("MBR_RESPONS_AMT")
        					.addColumn("NET_PAID_AMT")
        					.addColumn("SERVICE_DATE")
        					.addColumn("PROCESS_DATE")
        					.addColumn("REVERSAL_IND")
        					.addColumn("PHAR_NABP_ANON_ID")
        					.addColumn("PHAR_PAR_IND")
        					.addColumn("PHYS_NPI_ANON_ID")
        					.addColumn("service_month")
        					.addColumn("service_year")
        					.addColumn("MAIL_ORDER_IND")
        					.addColumn("COB_IND")
        					.addColumn("COMPOUND_IND")
        					.addColumn("DRUG_COV_STATUS_CD")
        					.addColumn("FORMULARY_IND")
        					.addColumn("GENERIC_AVAIL_CD")
        					.addColumn("PAYABLE_QTY")
        					.addColumn("DAW_CD")
        					.addColumn("REFILL_CD")
        					.addColumn("REFILL_NBR")
        					.addColumn("CICS_STAT_CD")
        					.addColumn("PCS_CLAIM_TYPE_CD")
        					.addColumn("GENERIC_IND")
        					.addColumn("DEA_SCH_IND")
        					.addColumn("BRAND_NAME")
        					.addColumn("GENERIC_NAME")
        					.addColumn("HUM_DRUG_CLASS_DESC")
        					.addColumn("GCN_ID")
        					.addColumn("GPI_DRUG_CLASS_DESC")
        					.addColumn("GPI_DRUG_CLASS_ID")
        					.addColumn("MAINT_DRUG_IND")
        					.addColumn("OTC_IND")
        					.addColumn("PHARM_DESC1")
        					.addColumn("PHARM_DESC2")
        					.addColumn("PHARM_ANON_ID")
        					.addColumn("CMPND_PHAR_IND")
        					.addColumn("LTC_PHAR_IND")
        					.addColumn("MAIL_ORDER_PHAR_IND")
        					.addColumn("SPCL_PHAR_IND")
        					.addColumn("PRESCRIBER_ANON_ID")
        					.addColumn("month_number")
        					.addColumn("Month")
        					.addColumn("MBR_PRESCRIBER_DISTANCE")
        					.addColumn("MBR_PHARMACY_DISTANCE")
        					.addColumn("PRESCRIBER_PHARMACY_DISTANCE")
        					.addColumn("DISP_CLASS_CD")
        					.addColumn("type")
        					.build().withHeader();
               
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir.toPath(), "*.csv")) {
            for (Path entry : stream) {
        		genTestUtils.logComments("Adding " + entry.getFileName().toString(), LOGLEVEL);


                MappingIterator<ObjectNode> it = csvMapper.readerFor(ObjectNode.class).with(bootstrapSchema)
                        .readValues(entry.toFile());
                long i = 0;
                while (it.hasNext()) {
                    ObjectNode jsonNode = it.next();
                    String jsonString = mapper.writeValueAsString(jsonNode);

                    String uri = "/" + ticket.getJobId() + "_" + Long.toString(i++) + ".json";
           			DocumentMetadataHandle metadata = new DocumentMetadataHandle();
        			// add a collection tag
        			metadata.getCollections().addAll(COLLECTION_NAME);
                    batcher.add(uri, metadata, new StringHandle(jsonString));
                    if (i % 100 == 0)
                		genTestUtils.logComments("Inserting JSON document " + uri, LOGLEVEL);
                }
                it.close();
            }
        }
        catch (IOException e) {
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

	@Test
	public void testLoadCSV() throws Exception {

		String methodName = new DataMovementSDKOperationsTest5() {
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
		loadCSV(client, COLLECTION_NAME);
		long countOfJsonDocs = countJSONDocuments(client, COLLECTION_NAME);
		assertEquals(TOTAL_JSON_DOCS_ADDED.longValue(), countOfJsonDocs);
		genTestUtils.logComments(new Date().toString() + " Loaded " + TOTAL_JSON_DOCS_ADDED + " documents.", LOGLEVEL);

		Date end = new Date();
		genTestUtils.logComments(end.toString() + " Ended Test Case: " + className, LOGLEVEL);
		genTestUtils.logComments(
				"Execution time for " + className + " is " + (end.getTime() - start.getTime()) / 1000 + " seconds.",
				LOGLEVEL);
	}

}