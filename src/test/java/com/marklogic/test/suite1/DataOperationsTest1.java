package com.marklogic.test.suite1;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStream;
import java.io.LineNumberReader;
import java.util.Date;

import org.junit.After;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.admin.ServerConfigurationManager;
import com.marklogic.client.admin.TransformExtensionsManager;
import com.marklogic.client.io.InputStreamHandle;
import com.marklogic.client.io.SearchHandle;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.query.DeleteQueryDefinition;
import com.marklogic.client.query.QueryManager;
import com.marklogic.client.query.StringQueryDefinition;
import com.marklogic.contentpump.ContentPump;
import com.marklogic.contentpump.utilities.OptionsFileUtil;

@Configuration
@PropertySource(value = { "classpath:contentpump.properties",
		"classpath:user.properties" }, ignoreResourceNotFound = true)
public class DataOperationsTest1 extends AbstractApiTest {

	@Value("${mlHost}")
	private String ML_HOST;
	@Value("${mlUser}")
	private String ML_USER;
	@Value("${mlPassword}")
	private String ML_PASSWORD;
	@Value("${mlcpDocPath}")
	private String DOC_PATH;
	@Value("${namePrefix}")
	private String NAME_PREFIX;
	@Value("${logLevel}")
	private String LOGLEVEL;

	private String COLLECTION_NAME = "";
	String DB_NAME = "";

	@After
	public void tearDown() {
	}

	public long countDocuments(DatabaseClient client, String COLLECTION_NAME) {
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
	public void testImportDelimitedText() throws Exception {

		String methodName = new DataOperationsTest1() {
		}.getClass().getEnclosingMethod().getName();
		String className = this.getClass().getName();

		COLLECTION_NAME = java.util.UUID.randomUUID().toString();
		GeneralUtils genTestUtils = new GeneralUtils();

		Date start = new Date();
		genTestUtils.logComments(start.toString() + " Started Test Case: " + methodName, LOGLEVEL);
		// Find DB Name
		DB_NAME = genTestUtils.getDBName(className, NAME_PREFIX);

		assertNotEquals("ERROR", DB_NAME);
		
				String cmd = "IMPORT -host " + ML_HOST + " -username " + ML_USER + " -password " + ML_PASSWORD
				+ " -input_file_path " + DOC_PATH + " -uri_id id -generate_uri false -output_uri_prefix "
				+ COLLECTION_NAME + " -output_uri_suffix " + ".xml" + " -input_file_type delimited_text"
				+ " -output_collections " + COLLECTION_NAME + " -port " + 8000 + " -database " + DB_NAME;

		genTestUtils.logComments("Executing MLCP Command is :: " + cmd, LOGLEVEL);

		String[] args = cmd.split(" ");

		String[] expandedArgs = null;
		expandedArgs = OptionsFileUtil.expandArguments(args);
		ContentPump.runCommand(expandedArgs);

		DatabaseClient client = DatabaseClientFactory.newClient(ML_HOST, 8000, DB_NAME,
				new DatabaseClientFactory.DigestAuthContext(ML_USER, ML_PASSWORD));

		long docsLoaded = countDocuments(client, COLLECTION_NAME);
		genTestUtils.logComments("Loaded " + docsLoaded + " documents in collection " + COLLECTION_NAME, LOGLEVEL);
		long linesFromSourceFile = 0L;
		// Count number of lines in the file
		File file = new File(DOC_PATH);
		if (file.exists()) {

			FileReader fr = new FileReader(file);
			LineNumberReader lnr = new LineNumberReader(fr);
			while (lnr.readLine() != null) {
				linesFromSourceFile++;
			}
			lnr.close();

		} else {
			genTestUtils.logComments("File does not exists!", LOGLEVEL);
			assertEquals(0, docsLoaded);
		}

		assertEquals((linesFromSourceFile - 1), docsLoaded);
		Date end = new Date();
		genTestUtils.logComments(end.toString() + " Ended Test Case: " + methodName, LOGLEVEL);
		genTestUtils.logComments(
				"Execution time for " + className + " is " + (end.getTime() - start.getTime()) / 1000 + " seconds.",
				LOGLEVEL);
	}
	
	@Test
	public void testImportDelimitedTextWithTransformation() throws Exception {

		String methodName = new DataOperationsTest1() {
		}.getClass().getEnclosingMethod().getName();
		String className = this.getClass().getName();

		COLLECTION_NAME = java.util.UUID.randomUUID().toString();
		GeneralUtils genTestUtils = new GeneralUtils();

		Date start = new Date();
		genTestUtils.logComments(start.toString() + " Started Test Case: " + methodName, LOGLEVEL);
		
		// Find DB Name
		DB_NAME = genTestUtils.getDBName(className, NAME_PREFIX);
		assertNotEquals("ERROR", DB_NAME);
		DatabaseClient client = DatabaseClientFactory.newClient(ML_HOST, 8000, DB_NAME,
				new DatabaseClientFactory.DigestAuthContext(ML_USER, ML_PASSWORD));

		/*Install Transforms - START */
		ServerConfigurationManager config = client.newServerConfigManager();
		TransformExtensionsManager transform = config.newTransformExtensionsManager();
		InputStream docStream = new FileInputStream(new File ("src/test/resources/lib/addMetaData.sjs"));
		InputStreamHandle handle = new InputStreamHandle(docStream);
		transform.writeJavascriptTransform("addMetaData", handle);
		docStream = new FileInputStream(new File ("src/test/resources/lib/mainTransform.sjs"));
		handle = new InputStreamHandle(docStream);
		transform.writeJavascriptTransform("mainTransform", handle);
		
		/*List Transforms - START */
		StringHandle textHandle = transform.readJavascriptTransform("mainTransform", new StringHandle());		
		assertNotNull(textHandle);
		textHandle = transform.readJavascriptTransform("addMetaData", new StringHandle());		
		assertNotNull(textHandle);
		/*List Transforms - END */
		
		String cmd = "IMPORT -host " + ML_HOST + " -username " + ML_USER + " -password " + ML_PASSWORD
		+ " -input_file_path " + DOC_PATH + " -uri_id id -generate_uri false -output_uri_prefix "
		+ COLLECTION_NAME + " -output_uri_suffix " + ".json" + " -input_file_type delimited_text "
		+ "-document_type json "
		+ "-output_collections " + COLLECTION_NAME + " -port " + 8000 + " -database " + DB_NAME
		+ " -transform_module /marklogic.rest.transform/mainTransform/assets/transform.sjs"
		+ " -transform_param" + ML_USER;

		genTestUtils.logComments("Executing MLCP Command is :: " + cmd, LOGLEVEL);

		String[] args = cmd.split(" ");

		String[] expandedArgs = null;
		expandedArgs = OptionsFileUtil.expandArguments(args);
		ContentPump.runCommand(expandedArgs);

		long docsLoaded = countDocuments(client, COLLECTION_NAME);
		genTestUtils.logComments("Loaded " + docsLoaded + " documents in collection " + COLLECTION_NAME, LOGLEVEL);
		long linesFromSourceFile = 0L;
		// Count number of lines in the file
		File file = new File(DOC_PATH);
		if (file.exists()) {

			FileReader fr = new FileReader(file);
			LineNumberReader lnr = new LineNumberReader(fr);
			while (lnr.readLine() != null) {
				linesFromSourceFile++;
			}
			lnr.close();

		} else {
			genTestUtils.logComments("File does not exists!", LOGLEVEL);
			assertEquals(0, docsLoaded);
		}
		assertEquals((linesFromSourceFile - 1), docsLoaded);
		/*Delete Transforms - START */
		transform.deleteTransform("mainTransform");
		textHandle = transform.readJavascriptTransform("mainTransform", new StringHandle());		
		assertNotNull(textHandle);
		transform.deleteTransform("addMetaData");
		textHandle = transform.readJavascriptTransform("addMetaData", new StringHandle());	
		assertNotNull(textHandle);
		/*Delete Transforms - START */
		Date end = new Date();
		genTestUtils.logComments(end.toString() + " Ended Test Case: " + methodName, LOGLEVEL);
		genTestUtils.logComments(
				"Execution time for " + className + " is " + (end.getTime() - start.getTime()) / 1000 + " seconds.",
				LOGLEVEL);
	}
}