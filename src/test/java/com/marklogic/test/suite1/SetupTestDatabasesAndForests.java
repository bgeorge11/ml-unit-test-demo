package com.marklogic.test.suite1;

import java.io.IOException;
import java.util.Date;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.eval.EvalResultIterator;
import com.marklogic.client.eval.ServerEvaluationCall;
import com.marklogic.mgmt.api.database.Database;
import com.marklogic.mgmt.api.forest.Forest;

@Configuration
@PropertySource(value = { "classpath:databaseXQuery.properties",
		"classpath:user.properties" }, ignoreResourceNotFound = true)
public class SetupTestDatabasesAndForests extends AbstractApiTest {

	@Value("${mlHost}")
	private String ML_HOST;
	@Value("${mlUser}")
	private String ML_USER;
	@Value("${mlPassword}")
	private String ML_PASSWORD;
	@Value("${namePrefix}")
	private String NAME_PREFIX;
	@Value("${logLevel}")
	private String LOGLEVEL;
	@Value("${numTestCases}")
	private String NUM_DATABASES;
	@Value("${forestsPerDatabase}")
	private String NUM_FORESTS;
	@Value("${testClassPatterns}")
	private String TEST_CLASS_PATTERNS;

	int INCREMENT = 0;

	private String createTemplateForest(String FOREST_NAME_PREFIX) {
		String templateForestName = "";

		DatabaseClient client = DatabaseClientFactory.newClient(ML_HOST, 8000, "Documents",
				new DatabaseClientFactory.DigestAuthContext(ML_USER, ML_PASSWORD));
		templateForestName = FOREST_NAME_PREFIX + "1";
		Forest forest = api.forest(templateForestName);
		assertFalse(forest.exists());
		forest.save();
		assertTrue(forest.exists());
		client.release();

		return templateForestName;
	}

	private String createTemplateDatabase(String DB_NAME_PREFIX) {
		String templateDatabaseName = "";

		DatabaseClient client = DatabaseClientFactory.newClient(ML_HOST, 8000, "Documents",
				new DatabaseClientFactory.DigestAuthContext(ML_USER, ML_PASSWORD));
		templateDatabaseName = DB_NAME_PREFIX + "1";
		Database db = api.db(templateDatabaseName);
		assertFalse(db.exists());
		db.setSchemaDatabase("Schemas");
		db.setSecurityDatabase("Security");
		db.save();
		assertTrue(db.exists());
		client.release();

		return templateDatabaseName;
	}

	private void createForests(String FOREST1_NAME_PREFIX, int numDatabases, String templateForestName)
			throws IOException {

		GeneralUtils testUtils = new GeneralUtils();
		DatabaseClient client = DatabaseClientFactory.newClient(ML_HOST, 8000, "Documents",
				new DatabaseClientFactory.DigestAuthContext(ML_USER, ML_PASSWORD));

		testUtils.logComments(new Date().toString() + " Starting Creating Forests..." + numDatabases, LOGLEVEL);

		ServerEvaluationCall theCall = client.newServerEval();
		String query = "import module namespace admin = \"http://marklogic.com/xdmp/admin\""
				+ "at \"/MarkLogic/admin.xqy\";declare variable $MAX := " + numDatabases + ";"
				+ "declare function local:forest($config, $counter as xs:int) {" + "if($counter le $MAX) then ("
				+ "let $new-config := admin:forest-copy($config, xdmp:forest(\"" + templateForestName + "\"), \""
				+ FOREST1_NAME_PREFIX + "\" || $counter, ())" + "return local:forest($new-config, ($counter + 1))"
				+ ") else $config" + "};" + "admin:save-configuration(local:forest(admin:get-configuration(), 2))";
		theCall.xquery(query);
		String response = theCall.evalAs(String.class);

		testUtils.logComments(new Date().toString() + " Competed Creating Forests...", LOGLEVEL);

		client.release();
	}

	private void createDatabases(String DB_NAME_PREFIX, int numDatabases, String templateDatabaseName)
			throws IOException {

		GeneralUtils testUtils = new GeneralUtils();
		DatabaseClient client = DatabaseClientFactory.newClient(ML_HOST, 8000, "Documents",
				new DatabaseClientFactory.DigestAuthContext(ML_USER, ML_PASSWORD));

		testUtils.logComments(new Date().toString() + " Starting Creating Databases..." + numDatabases, LOGLEVEL);

		ServerEvaluationCall theCall = client.newServerEval();
		String query = "xquery version \"1.0-ml\";"
				+ "import module namespace admin = \"http://marklogic.com/xdmp/admin\""
				+ "at \"/MarkLogic/admin.xqy\";declare variable $MAX := " + numDatabases + ";"
				+ "declare function local:copyDatabases($config, $counter as xs:int) {" + "if($counter le $MAX) then ("
				+ "let $new-config := admin:database-copy($config, xdmp:database(\"" + templateDatabaseName + "\"), \""
				+ DB_NAME_PREFIX + "\" || $counter)" + "return local:copyDatabases($new-config, ($counter + 1))"
				+ ") else $config" + "};"
				+ "admin:save-configuration(local:copyDatabases(admin:get-configuration(), 2))";
		theCall.xquery(query);
		/*
		 * TODO the above is not the right way of calling external module.
		 */
		String response = theCall.evalAs(String.class);

		testUtils.logComments(new Date().toString() + " Competed Creating Databases...", LOGLEVEL);
		client.release();

	}

	public int countDatabases(String DB_NAME_PREFIX) {

		GeneralUtils testUtils = new GeneralUtils();
		DatabaseClient client = DatabaseClientFactory.newClient(ML_HOST, 8000, "Documents",
				new DatabaseClientFactory.DigestAuthContext(ML_USER, ML_PASSWORD));
		testUtils.logComments(new Date().toString() + " Starting Counting Databases...", LOGLEVEL);

		ServerEvaluationCall theCall = client.newServerEval();
		int numDatabases = 0;

		String query = "xquery version \"1.0-ml\";" + "for $db-name in xdmp:database-name(xdmp:databases()) "
				+ "where fn:starts-with($db-name, \"" + DB_NAME_PREFIX + "\")" + "return $db-name";

		theCall.xquery(query);
		EvalResultIterator response = theCall.eval();

		while (response.hasNext()) {
			response.next(); // Just pass, no need to do anything now !
			numDatabases++;
		}

		client.release();

		return numDatabases;
	}

	@Test
	public void createTestForests() throws ClassNotFoundException, IOException {

		String methodName = new SetupTestDatabasesAndForests() {
		}.getClass().getEnclosingMethod().getName();
		String packageName = new SetupTestDatabasesAndForests() {
		}.getClass().getPackage().getName();
		String className = this.getClass().getName();

		GeneralUtils testUtils = new GeneralUtils();

		Date start = new Date();
		testUtils.logComments(start.toString() + " Starting creating forests..." + methodName, LOGLEVEL);

		String FOREST1_NAME_PREFIX = NAME_PREFIX + "forest1-";

		if (Integer.parseInt(NUM_DATABASES) > 0) {

			createForests(FOREST1_NAME_PREFIX, Integer.parseInt(NUM_DATABASES) - 1,
					createTemplateForest(FOREST1_NAME_PREFIX));
		} else {
			createForests(FOREST1_NAME_PREFIX, testUtils.countTestCases(TEST_CLASS_PATTERNS, packageName),
					createTemplateForest(FOREST1_NAME_PREFIX));
		}

		// If the test reached here, then assertTrue. TODO - Count the forests
		// again
		// and then assert true.
		assertTrue(true);
		Date end = new Date();
		testUtils.logComments(end.toString() + " Created Forests " + methodName, LOGLEVEL);
		testUtils.logComments(
				"Execution time for " + methodName + " is " + (end.getTime() - start.getTime()) / 1000 + " seconds.",
				LOGLEVEL);
	}

	@Test
	public void createTestDatabases() throws NumberFormatException, IOException {

		String methodName = new SetupTestDatabasesAndForests() {
		}.getClass().getEnclosingMethod().getName();
		String packageName = new SetupTestDatabasesAndForests() {
		}.getClass().getPackage().getName();
		String className = this.getClass().getName();

		String DB_NAME_PREFIX = "";
		GeneralUtils testUtils = new GeneralUtils();
		int numDatabaseToCreate = Integer.parseInt(NUM_DATABASES);

		Date start = new Date();
		testUtils.logComments(start.toString() + " Starting creating Databases..." + methodName, LOGLEVEL);

		DB_NAME_PREFIX = NAME_PREFIX;
		if (numDatabaseToCreate > 0) {
			createDatabases(DB_NAME_PREFIX, Integer.parseInt(NUM_DATABASES) - 1,
					createTemplateDatabase(DB_NAME_PREFIX));
		} else {
			numDatabaseToCreate = testUtils.countTestCases(TEST_CLASS_PATTERNS, packageName);
			createDatabases(DB_NAME_PREFIX, numDatabaseToCreate, createTemplateDatabase(DB_NAME_PREFIX));

		}
		// Count the databases back and then assert true
		assertEquals(countDatabases(DB_NAME_PREFIX), numDatabaseToCreate);
		Date end = new Date();
		testUtils.logComments(end.toString() + " Created Databases " + methodName, LOGLEVEL);
		testUtils.logComments(
				"Execution time for " + methodName + " is " + (end.getTime() - start.getTime()) / 1000 + " seconds.",
				LOGLEVEL);
	}

}