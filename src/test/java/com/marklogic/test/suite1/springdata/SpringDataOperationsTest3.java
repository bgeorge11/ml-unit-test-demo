package com.marklogic.test.suite1.springdata;

import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.DatabaseClientFactory.DigestAuthContext;
import com.marklogic.client.query.StructuredQueryBuilder;
import com.marklogic.test.suite1.AbstractApiTest;
import com.marklogic.test.suite1.GeneralUtils;
import com.marklogic.test.suite1.springdata.Person;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import io.github.malteseduck.springframework.data.marklogic.core.MarkLogicOperations;
import io.github.malteseduck.springframework.data.marklogic.core.MarkLogicTemplate;

import static com.marklogic.client.DatabaseClientFactory.newClient;

import java.io.File;
import java.util.Date;
import java.util.List;

@Configuration
@PropertySource(value = { "classpath:suite1.properties", "classpath:user.properties" }, ignoreResourceNotFound = true)

public class SpringDataOperationsTest3 extends AbstractApiTest {

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

	private String COLLECTION_NAME = "";
	String DB_NAME = "";

	private static final StructuredQueryBuilder qb = new StructuredQueryBuilder();

	@Test
	public void doSpringDataTest() {

		String methodName = new SpringDataOperationsTest3() {
		}.getClass().getEnclosingMethod().getName();
		String className = this.getClass().getName();
		GeneralUtils genTestUtils = new GeneralUtils();

		Date start = new Date();
		genTestUtils.logComments(start.toString() + " Started Test Case: " + className, LOGLEVEL);

		// Find DB Name
		DB_NAME = genTestUtils.getDBName(className, NAME_PREFIX);
		assertNotEquals("ERROR", DB_NAME);

		MarkLogicOperations ops = new MarkLogicTemplate(
				newClient(ML_HOST, 8000, DB_NAME, new DatabaseClientFactory.DigestAuthContext(ML_USER, ML_PASSWORD)));
		ops.write(new Person(1, "Bobby", 23));
		ops.write(new Person(2, "Sam", 26));
		ops.write(new Person(3, "Tara", 33));

		Person bobby = ops.searchOne(qb.value(qb.jsonProperty("name"), "Bobby"), Person.class);
		assertEquals(bobby.getName(), "Bobby");
		List<Person> person = ops.search(qb.value(qb.jsonProperty("age"), 26), Person.class);
		assertEquals(person.get(0).getName(), "Sam");

		Date end = new Date();
		genTestUtils.logComments(end.toString() + " Ended Test Case: " + className, LOGLEVEL);
		genTestUtils.logComments(
				"Execution time for " + className + " is " + (end.getTime() - start.getTime()) / 1000 + " seconds.",
				LOGLEVEL);

	}

}
