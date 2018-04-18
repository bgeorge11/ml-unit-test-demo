package com.marklogic.test.suite1;

import java.io.IOException;
import java.util.Date;

import org.junit.After;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.eval.ServerEvaluationCall;

@Configuration
@PropertySource(value = { "classpath:databaseXQuery.properties",
		"classpath:user.properties" }, ignoreResourceNotFound = true)
public class StepCRunner extends AbstractApiTest {

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

	private void deleteForests(String FOREST_NAME_PREFIX) throws IOException {

		DatabaseClient client = DatabaseClientFactory.newClient(ML_HOST, 8000, "Documents",
				new DatabaseClientFactory.DigestAuthContext(ML_USER, ML_PASSWORD));

		ServerEvaluationCall theCall = client.newServerEval();
		String query = "xquery version \"1.0-ml\";"
				+ "import module namespace admin = \"http://marklogic.com/xdmp/admin\""
				+ "at \"/MarkLogic/admin.xqy\";declare function local:forest-ids($prefix ) {"
				+ "for $f in xdmp:forests()"
				+ "return if(fn:starts-with(xdmp:forest-name($f), $prefix)) then $f else ()"
				+ "};let $forest-ids := local:forest-ids(\"" + FOREST_NAME_PREFIX + "\")"
				+ "return if($forest-ids) then "
				+ "admin:save-configuration(admin:forest-delete(admin:get-configuration(), $forest-ids, fn:true())) else ()";
		theCall.xquery(query);
		/*
		 * TODO the above is not the right way of calling external module.
		 */
		String response = theCall.evalAs(String.class);
		client.release();

	}

	private void deleteDatabases(String DB_NAME_PREFIX) throws IOException {

		DatabaseClient client = DatabaseClientFactory.newClient(ML_HOST, 8000, "Documents",
				new DatabaseClientFactory.DigestAuthContext(ML_USER, ML_PASSWORD));

		GeneralUtils testUtils = new GeneralUtils();
		ServerEvaluationCall theCall = client.newServerEval();
		String query = "xquery version \"1.0-ml\";"
				+ "import module namespace admin = \"http://marklogic.com/xdmp/admin\""
				+ "at \"/MarkLogic/admin.xqy\";declare function local:database-ids($prefix ) {"
				+ "for $f in xdmp:databases()"
				+ "return if(fn:starts-with(xdmp:database-name($f), $prefix)) then $f else ()"
				+ "};let $database-ids := local:database-ids(\"" + DB_NAME_PREFIX + "\")"
				+ "return if($database-ids) then "
				+ "admin:save-configuration(admin:database-delete(admin:get-configuration(), $database-ids)) else ()";
		theCall.xquery(query);
		/*
		 * TODO the above is not the right way of calling external module.
		 */
		String response = theCall.evalAs(String.class);
		client.release();

	}

	@After
	public void deleteTestForests() throws ClassNotFoundException, IOException {

		String methodName = new StepCRunner() {
		}.getClass().getEnclosingMethod().getName();
		String packageName = new StepCRunner() {
		}.getClass().getPackage().getName();
		String className = this.getClass().getName();

		GeneralUtils testUtils = new GeneralUtils();

		Date start = new Date();
		System.out.println(start.toString() + " Starting deleting forests..." + methodName);

		String FOREST1_NAME_PREFIX = NAME_PREFIX + "forest1-";
		String FOREST2_NAME_PREFIX = NAME_PREFIX + "forest2-";
		deleteForests(FOREST1_NAME_PREFIX);
		Date end = new Date();
		System.out.println(end.toString() + " Deleted Forests " + methodName);
		System.out.println(
				"Execution time for " + methodName + " is " + (end.getTime() - start.getTime()) / 1000 + " seconds.");
	}

	@Test
	public void deleteTestDatabases() throws NumberFormatException, IOException {

		String methodName = new StepCRunner() {
		}.getClass().getEnclosingMethod().getName();
		String packageName = new StepCRunner() {
		}.getClass().getPackage().getName();
		String className = this.getClass().getName();

		String prefix = "";
		String DB_NAME_PREFIX = "";
		GeneralUtils testUtils = new GeneralUtils();
		int numDatabaseToCreate = Integer.parseInt(NUM_DATABASES);

		Date start = new Date();
		System.out.println(start.toString() + " Starting deleting Databases..." + methodName);

		DB_NAME_PREFIX = NAME_PREFIX;

		deleteDatabases(DB_NAME_PREFIX);

		Date end = new Date();
		System.out.println(end.toString() + " Deleted Databases " + methodName);
		System.out.println(
				"Execution time for " + methodName + " is " + (end.getTime() - start.getTime()) / 1000 + " seconds.");
	}

}