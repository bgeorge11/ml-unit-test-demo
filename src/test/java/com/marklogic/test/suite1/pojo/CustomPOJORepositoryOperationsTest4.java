package com.marklogic.test.suite1.pojo;

import java.io.FileNotFoundException;
import java.util.Date;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.admin.QueryOptionsManager;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.DocumentMetadataHandle.DocumentProperties;
import com.marklogic.client.io.SearchHandle;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.pojo.PojoRepository;
import com.marklogic.client.query.MatchDocumentSummary;
import com.marklogic.client.query.QueryManager;
import com.marklogic.client.query.StringQueryDefinition;
import com.marklogic.client.query.StructuredQueryBuilder;
import com.marklogic.client.query.StructuredQueryDefinition;
import com.marklogic.test.suite1.AbstractApiTest;
import com.marklogic.test.suite1.GeneralUtils;

// illustrates how to write POJOs to the database
@Configuration
@PropertySource(value = { "classpath:contentpump.properties",
		"classpath:user.properties" }, ignoreResourceNotFound = true)
public class CustomPOJORepositoryOperationsTest4 extends AbstractApiTest {

	@Value("${mlHost}")
	private String ML_HOST;
	@Value("${mlUser}")
	private String ML_USER;
	@Value("${mlPassword}")
	private String ML_PASSWORD;
	@Value("${pojoPath}")
	private String POJO_PATH;
	@Value("${namePrefix}")
	private String NAME_PREFIX;
	@Value("${logLevel}")
	private String LOGLEVEL;

	private String COLLECTION_NAME = "";
	String DB_NAME = "";

	@Test
	public void doCustomePojoRepositoryOperationsTest() throws FileNotFoundException {

		String methodName = new CustomPOJORepositoryOperationsTest4() {
		}.getClass().getEnclosingMethod().getName();
		String className = this.getClass().getName();
		GeneralUtils genTestUtils = new GeneralUtils();

		Date start = new Date();
		genTestUtils.logComments(start.toString() + " Started Test Case: " + methodName, LOGLEVEL);

		// Find DB Name
		DB_NAME = genTestUtils.getDBName(className, NAME_PREFIX);
		assertNotEquals("ERROR", DB_NAME);

		COLLECTION_NAME = java.util.UUID.randomUUID().toString();
		// create the client
		DatabaseClient client = DatabaseClientFactory.newClient(ML_HOST, 8000, DB_NAME,
				new DatabaseClientFactory.DigestAuthContext(ML_USER, ML_PASSWORD));

		User shauna = new User();
		shauna.setName("Shauna Weber");
		shauna.setAddress("760 Forest Place, Glenshaw, Michigan, 1175");
		shauna.setAbout(
				"Kitsch fingerstache XOXO, Carles chambray 90's meh cray disrupt Tumblr. Biodiesel craft beer sartorial meh put a bird on it, literally keytar blog vegan paleo. Chambray messenger bag +1 hoodie, try-hard actually banjo bespoke distillery pour-over Godard Thundercats organic. Kitsch wayfarers Pinterest American Apparel. Hella Shoreditch blog, shabby chic iPhone tousled paleo before they sold out keffiyeh Portland Marfa twee dreamcatcher. 8-bit Vice post-ironic plaid. Cornhole Schlitz blog direct trade lomo Pinterest.");
		shauna.setActive(true);
		shauna.setBalance(2774.31);
		shauna.setGender("female");
		shauna.setAge(29);
		

		User peters = new User();
		peters.setName("Peters Barnett");
		peters.setAddress("749 Green Street, Tyro, Illinois, 2856");
		peters.setAbout(
				"Letterpress Echo Park fashion axe occupy whatever before they sold out, Pinterest pickled cliché. Ethnic stumptown food truck wolf, ethical Helvetica Marfa hashtag. Echo Park photo booth banh mi ennui, organic VHS 8-bit fixie. Skateboard irony dreamcatcher mlkshk iPhone cliche. Flannel ennui YOLO artisan tofu. Hashtag irony Shoreditch letterpress, selvage scenester YOLO. Locavore fap bicycle rights, drinking vinegar Tonx bespoke paleo 3 wolf moon readymade direct trade ugh wolf asymmetrical beard plaid.");
		peters.setActive(false);
		peters.setBalance(1787.45);
		peters.setGender("male");
		peters.setAge(38);
		peters.getTags().add(new Tag("ex"));
		peters.getTags().add(new Tag("ex"));
		peters.getTags().add(new Tag("ut"));
		peters.getTags().add(new Tag("exercitation"));
		peters.getTags().add(new Tag("Lorem"));
		peters.getTags().add(new Tag("magna"));
		peters.getTags().add(new Tag("non"));
		peters.getTags().add(new Tag("aute"));
		peters.getTags().add(new Tag("nisi"));
		
		DocumentMetadataHandle metaHandle = new DocumentMetadataHandle();
		DocumentProperties props = metaHandle.getProperties();
		props.put("prop1", "value1");
		props.put("prop2", "value2");
		
		DAOFactory daoFactory = new DAOFactory();
		PojoRepository<User,String> userRepo = daoFactory.getPojoRepository(client, User.class, props);

		shauna.setGUID("shauna_"+COLLECTION_NAME);
		userRepo.write(shauna,COLLECTION_NAME);
		
		props.put("prop3", "value3");
		//props.clear(); /*In case to test empty metadata properties*/
		peters.setGUID("peters_"+COLLECTION_NAME);
		userRepo.write(peters, COLLECTION_NAME);
		
		User resultUser = userRepo.read(peters.getGUID());
		
		assertEquals("Peters Barnett", resultUser.getName());
		
		/*Query by meta data myMeta1 which was added through POJORepository.write. 
		 * A metadata field on myMeta1 is a prerequisite */
		
		String optionsName = "myOptions"; 
		QueryOptionsManager optionsMgr = client.newServerConfigManager().newQueryOptionsManager();
		String opts1 = "<search:options xmlns:search='http://marklogic.com/appservices/search'>"
						+ "<search:constraint name=\"by\">"
						+ "<search:word>"
						+ "<search:field name=\"myMeta1\"/>"
						+ "</search:word>"
						+ "</search:constraint>"
						+ "<search:additional-query>"
						+ "<cts:collection-query xmlns:cts=\"http://marklogic.com/cts\">"
						+ "<cts:uri>"+ COLLECTION_NAME + "</cts:uri>"
				        + " </cts:collection-query>"
				        + "</search:additional-query>"
						+ "</search:options>";

		StringHandle handle = new StringHandle(opts1);
		optionsMgr.writeOptions(optionsName, handle);
				/**** SEARCH ****/
		// create a manager for searching
		QueryManager queryMgr = client.newQueryManager();
		// create a search definition using the "tutorial" options
		StringQueryDefinition query = queryMgr.newStringDefinition("myOptions");
		query.setCriteria("by:myValue1");
		// run the search
		SearchHandle resultsHandle = queryMgr.search(query, new SearchHandle());
		MatchDocumentSummary[] results = resultsHandle.getMatchResults();
		assertEquals(2,results.length);
		optionsMgr.deleteOptions(optionsName);
		
		/* Another way of Search */
        StructuredQueryBuilder sqb = queryMgr.newStructuredQueryBuilder();
        StructuredQueryDefinition query1 = sqb.word(sqb.field("myMeta1"),"myValue1");
        query1.setCollections(COLLECTION_NAME);
        resultsHandle = queryMgr.search(query1, new SearchHandle());
		results = resultsHandle.getMatchResults();
		assertEquals(2,results.length);
			
		Date end = new Date();
		genTestUtils.logComments(end.toString() + " Ended Test Case: " + methodName, LOGLEVEL);
		genTestUtils.logComments(
				"Execution time for " + methodName + " is " + (end.getTime() - start.getTime()) / 1000 + " seconds.",
				LOGLEVEL);
	}
}
