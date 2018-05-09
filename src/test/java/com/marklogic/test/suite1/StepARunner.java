package com.marklogic.test.suite1;

import java.io.IOException;
import java.util.Date;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.ParallelComputer;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.eval.ServerEvaluationCall;

@Configuration
@PropertySource(value = { "classpath:databaseXQuery.properties",
		"classpath:user.properties" }, ignoreResourceNotFound = true)
public class StepARunner extends AbstractApiTest {

	@Value("${mlHost}")
	private String ML_HOST;
	@Value("${mlUser}")
	private String ML_USER;
	@Value("${mlPassword}")
	private String ML_PASSWORD;
	@Value("${namePrefix}")
	private String NAME_PREFIX;
	@Value("${numTestCases}")
	private String NUM_DATABASES;
	@Value("${logLevel}")
	private String LOGLEVEL;
	@Value("${forestsPerDatabase}")
	private String NUM_FORESTS;
	@Value("${testClassPatterns}")
	private String TEST_CLASS_PATTERNS;
	int INCREMENT = 0;

	private void attachForests(String DB_NAME_PREFIX, String FOREST1_NAME_PREFIX, int numDatabases) throws IOException {

		GeneralUtils testUtils = new GeneralUtils();
		DatabaseClient client = DatabaseClientFactory.newClient(ML_HOST, 8000, "Documents",
				new DatabaseClientFactory.DigestAuthContext(ML_USER, ML_PASSWORD));

		testUtils.logComments(new Date().toString() + " Starting Attaching Forests...", LOGLEVEL);

		ServerEvaluationCall theCall = client.newServerEval();
		String query = "xquery version \"1.0-ml\";"
				+ "import module namespace admin = \"http://marklogic.com/xdmp/admin\""
				+ "at \"/MarkLogic/admin.xqy\";declare variable $MAX :=" + (numDatabases) + ";"
				+ "declare function local:attachForests($config, $counter as xs:int) {" + "if($counter le $MAX) then ("
				+ "let $new-config := admin:database-attach-forest($config, xdmp:database(\"" + DB_NAME_PREFIX
				+ "\"||$counter), xdmp:forest(\"" + FOREST1_NAME_PREFIX + "\"||$counter))"
				+ "return local:attachForests($new-config, ($counter + 1))" + ") else $config" + "};"
				+ "admin:save-configuration(local:attachForests(admin:get-configuration(), 1))";

		theCall.xquery(query);
		String response = theCall.evalAs(String.class);

		testUtils.logComments(new Date().toString() + " Competed Attaching Forests...", LOGLEVEL);

		client.release();

	}

	@Before
	public void SetUpTestDatabasesAndForests() throws NumberFormatException, IOException {

		Class[] cls = { SetupTestDatabasesAndForests.class };
		GeneralUtils testUtils = new GeneralUtils();
		Date startTime = new Date();
		// Parallel among methods
		Result results = JUnitCore.runClasses(ParallelComputer.methods(), cls);

		System.out.println("-------------SET UP DATABASES AND FORESTS RESULTS---------------");
		System.out.println("Total RunCount = " + results.getRunCount());
		System.out.println("Failure Count = " + results.getFailureCount());
		System.out.println("Run Time = " + results.getRunTime() / 1000 + " seconds");
		List<Failure> failure = results.getFailures();

		for (int i = 0; i < failure.size(); i++) {
			System.out.println("Setup Database Failure details " + failure.get(i).getTestHeader() + " "
					+ failure.get(i).getException());
		}

		assertEquals(0, results.getFailureCount());

	}

	@Test
	public void attachTestForestsAndDatabases() throws NumberFormatException, IOException {

		String methodName = new StepARunner() {
		}.getClass().getEnclosingMethod().getName();
		String className = this.getClass().getName();
		String packageName = new SetupTestDatabasesAndForests() {
		}.getClass().getPackage().getName();

		GeneralUtils testUtils = new GeneralUtils();

		int numDatabases = Integer.parseInt(NUM_DATABASES);

		Date start = new Date();
		testUtils.logComments(start.toString() + " Starting attaching forests..." + methodName, LOGLEVEL);

		String DB_NAME_PREFIX = NAME_PREFIX;
		String FOREST1_NAME_PREFIX = NAME_PREFIX + "forest1-";
		String FOREST2_NAME_PREFIX = NAME_PREFIX + "forest2-";
		if (numDatabases > 0) {
			attachForests(DB_NAME_PREFIX, FOREST1_NAME_PREFIX, numDatabases);
		} else {
			numDatabases = testUtils.countTestCases(TEST_CLASS_PATTERNS, packageName);
			attachForests(DB_NAME_PREFIX, FOREST1_NAME_PREFIX, numDatabases);

		}
		Date end = new Date();
		testUtils.logComments(end.toString() + " Attached Forests " + methodName, LOGLEVEL);
		testUtils.logComments(
				"Execution time for " + methodName + " is " + (end.getTime() - start.getTime()) / 1000 + " seconds.",
				LOGLEVEL);
	}

}
